"""Watching many servers at once, and reporting each transaction exactly once.

Why bother with more than one server? Because a transaction does not reach every
server at the same moment. It starts somewhere and spreads outward, so whichever
server happens to be near its origin hears about it first. Listening to one
server means waiting for the transaction to travel to that one; listening to ten
means hearing it as soon as the luckiest of the ten does.

How much that is worth depends on where you are and which servers answer you.

One structural decision worth knowing about: reader threads do the least possible
work - hash the bytes, note the time, hand off. Working out what a transaction
*says* costs enough to delay the next message on that connection, so decoding
happens on a separate thread and the arrival time is recorded before it.
"""

import queue
import random
import socket
import threading
import time
from collections import OrderedDict

from . import connection, discovery, identity, transactions, wire

# How many transaction ids to remember, so we can tell new from repeated. Well
# past a minute of traffic, and small enough to stay tiny in memory.
REMEMBER_THIS_MANY = 200_000

# Wait this long before retrying a server that dropped us, doubling each time.
FIRST_RETRY_WAIT_SECONDS = 2
LONGEST_RETRY_WAIT_SECONDS = 60

# Transactions waiting to be decoded. Deep enough to ride out a burst, shallow
# enough that a wedged consumer is visible as drops rather than as memory growth.
BACKLOG_LIMIT = 20_000

# How often to check whether we have lost too many servers, and top up.
TOP_UP_EVERY_SECONDS = 120

# Never start more listener threads than this multiple of what was asked for.
MOST_THREADS_PER_SERVER_ASKED = 6

STOP_SENDING_ME_THIS = 55

# Hang up and redial after this long, so a connection that has gone quiet
# without closing is not kept forever.
HOW_LONG_ONE_CONNECTION_LIVES = 3600


class TransactionFeed:
    """Connects to several servers and calls you back for each new transaction.

    Typical use:

        feed = TransactionFeed(on_transaction=print, only_types=["OfferCreate"])
        feed.start(number_of_servers=10)
        ...
        feed.stop()

    `on_transaction(details)` is called once per transaction, on a worker thread,
    with the decoded transaction plus `_id`, `_heard_from` and `_heard_at`. If it
    raises, the feed stops and keeps the exception in `failure`; `wait()` returns.
    """

    def __init__(self, on_transaction, only_types=None):
        self.on_transaction = on_transaction
        self.only_types = set(only_types) if only_types else None
        self.failure: Exception | None = None

        self._already_seen: OrderedDict[str, None] = OrderedDict()
        self._lock = threading.Lock()
        self._please_stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._links: dict[str, object] = {}
        self._waiting: queue.Queue = queue.Queue(maxsize=BACKLOG_LIMIT)
        self._dialled: set[tuple[str, int]] = set()
        self._wanted = 0

        self._connected: set[str] = set()
        self.counts = {
            "reported": 0,
            "repeats_ignored": 0,
            "unreadable": 0,
            "compressed_skipped": 0,
            "squelched": 0,
            "dropped_backlog": 0,
            "connection_failures": 0,
        }

    # ---------------------------------------------------------------- public

    @property
    def connected_servers(self) -> set[str]:
        """Which servers are answering right now."""
        with self._lock:
            return set(self._connected)

    def start(self, number_of_servers: int = 10, addresses=None) -> None:
        """Find servers and begin listening. Returns immediately.

        Pass `addresses` as a list of (host, port) to listen to specific servers
        instead of whatever the network offers - useful once you know which ones
        answer you first, from the `_heard_from` field.
        """
        self._wanted = number_of_servers if addresses is None else len(addresses)

        decoder = threading.Thread(target=self._decode_forever, daemon=True,
                                   name="xrplwatch-decoder")
        decoder.start()
        self._threads.append(decoder)

        if addresses is None:
            # Dial more than we need, because busy servers turn most callers away.
            candidates = discovery.find_servers(number_of_servers * 3)
            self._dial(candidates[: number_of_servers * 2])
            keeper = threading.Thread(target=self._top_up_forever, daemon=True,
                                      name="xrplwatch-supervisor")
            keeper.start()
            self._threads.append(keeper)
        else:
            self._dial(list(addresses))

    def wait(self, timeout: float | None = None) -> bool:
        """Block until the feed stops - because `stop()` was called, or because
        `on_transaction` raised. Returns False if `timeout` ran out first."""
        return self._please_stop.wait(timeout)

    def stop(self, patience_seconds: float = 3.0) -> None:
        """Ask every connection to close, and wait briefly for threads to finish."""
        self._please_stop.set()

        with self._lock:
            links = list(self._links.values())
            self._links.clear()
        for link in links:
            # Shut the underlying socket rather than the TLS session: it makes any
            # thread parked in select return at once, instead of waiting out its
            # timeout. The reader thread closes the link itself on the way out.
            try:
                link.sock_shutdown(socket.SHUT_RDWR)
            except Exception:
                pass

        deadline = time.time() + patience_seconds
        for thread in self._threads:
            thread.join(timeout=max(0.0, deadline - time.time()))

    # --------------------------------------------------------------- private

    def _tally(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self.counts[name] += amount

    def _dial(self, servers) -> None:
        cap = max(1, self._wanted) * MOST_THREADS_PER_SERVER_ASKED
        for address, port in servers:
            if (address, port) in self._dialled or len(self._dialled) >= cap:
                continue
            self._dialled.add((address, port))
            thread = threading.Thread(target=self._listen_forever, args=(address, port),
                                      daemon=True, name=f"xrplwatch-{address}")
            thread.start()
            self._threads.append(thread)

    def _top_up_forever(self) -> None:
        """Replace servers that have dropped us, so a long run does not decay."""
        while not self._please_stop.wait(TOP_UP_EVERY_SECONDS):
            if len(self.connected_servers) >= self._wanted:
                continue
            try:
                self._dial(discovery.find_servers(self._wanted * 3))
            except Exception:
                continue        # the seeds are unreachable; try again next time

    def _is_new(self, transaction_id: str) -> bool:
        """True the first time we see an id, False every time after."""
        with self._lock:
            if transaction_id in self._already_seen:
                self.counts["repeats_ignored"] += 1
                return False

            self._already_seen[transaction_id] = None
            while len(self._already_seen) > REMEMBER_THIS_MANY:
                self._already_seen.popitem(last=False)   # forget the oldest
            return True

    def _decode_forever(self) -> None:
        """Work out what each new transaction says, off the reading threads."""
        while not self._please_stop.is_set():
            try:
                item = self._waiting.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._report(*item)
            except Exception as problem:
                # The callback is the caller's code. Stop the whole feed and say
                # why, rather than let it run on with nobody listening.
                self.failure = problem
                self._please_stop.set()

    def _report(self, transaction_id: str, raw: bytes, server: str, heard_at: float) -> None:
        try:
            details = transactions.read_transaction(raw)
        except Exception:
            self._tally("unreadable")
            return

        if self.only_types and details.get("TransactionType") not in self.only_types:
            return

        details["_id"] = transaction_id
        details["_heard_from"] = server
        details["_heard_at"] = round(heard_at, 6)

        self._tally("reported")
        self.on_transaction(details)

    def _listen_forever(self, address: str, port: int) -> None:
        """Stay connected to one server, reconnecting whenever it drops us."""
        server = f"{address}:{port}"
        retry_wait = FIRST_RETRY_WAIT_SECONDS

        while not self._please_stop.is_set():
            try:
                peer = connection.open_connection(address, port, identity.create_identity())
            except Exception:
                self._tally("connection_failures")
            else:
                with self._lock:
                    self._links[server] = peer.link
                    self._connected.add(server)
                retry_wait = FIRST_RETRY_WAIT_SECONDS
                try:
                    self._read_until_dropped(peer.link, peer.leftover, server)
                finally:
                    with self._lock:
                        self._links.pop(server, None)
                        self._connected.discard(server)
                    try:
                        peer.link.close()
                    except Exception:
                        pass

            if self._please_stop.is_set():
                return

            # Jittered, so twenty threads do not all knock on the door together.
            self._please_stop.wait(retry_wait * (0.5 + random.random()))
            retry_wait = min(retry_wait * 2, LONGEST_RETRY_WAIT_SECONDS)

    def _read_until_dropped(self, link, leftover: bytes, server: str) -> None:
        stop_at = time.time() + HOW_LONG_ONE_CONNECTION_LIVES

        for heard_at, kind, content in wire.split_into_messages(link, leftover, stop_at):
            if self._please_stop.is_set():
                return

            if content is None:           # compressed, and we do not unpack those
                self._tally("compressed_skipped")
                continue

            if kind == connection.ARE_YOU_ALIVE:
                try:
                    connection.answer_ping(link, content)
                except Exception:
                    return                # connection is gone
                continue

            if kind == STOP_SENDING_ME_THIS:
                # The server is squelching us for some validator's messages. It
                # does not stop transactions, but a rising count here is the first
                # sign a peer is trimming what it tells us.
                self._tally("squelched")
                continue

            for raw in transactions.transactions_in_message(kind, content):
                new_id = transactions.transaction_id(raw)
                if not self._is_new(new_id):
                    continue
                try:
                    self._waiting.put_nowait((new_id, raw, server, heard_at))
                except queue.Full:
                    self._tally("dropped_backlog")
