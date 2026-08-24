"""Print XRP Ledger transactions as soon as they happen.

    uv run python xrpl_feed.py
    uv run python xrpl_feed.py --watch trades
    uv run python xrpl_feed.py --watch OfferCreate,AMMCreate --servers 12

One transaction per line, written as JSON, so you can send it anywhere:

    uv run python xrpl_feed.py --watch trades | jq -c '{type: .TransactionType}'

Give it a minute to warm up. Most servers are busy and turn new connections away,
so it dials extras and keeps retrying until enough answer. The status line on
stderr says how many are actually answering; `refused` counting up while
`connected` stays low is normal and not an error.
"""

import argparse
import json
import os
import sys
import time

from xrplwatch import TransactionFeed, transactions

STATUS_LINE_EVERY_SECONDS = 30


def parse_what_to_watch(text: str) -> list[str] | None:
    """Turn --watch into a list of official transaction names.

    Accepts friendly groups ("trades") and official names ("OfferCreate"), mixed.
    An empty value means watch everything.
    """
    if not text.strip():
        return None

    wanted: list[str] = []
    for piece in text.split(","):
        piece = piece.strip()
        if piece:
            wanted.extend(transactions.names_for(piece))
    return wanted or None


def main() -> None:
    groups = ", ".join(transactions.WHAT_PEOPLE_USUALLY_WATCH)
    parser = argparse.ArgumentParser(
        description="Print XRP Ledger transactions as soon as they happen.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Groups you can pass to --watch: {groups}\n"
               f"Or use official names directly, e.g. --watch OfferCreate,Payment",
    )
    parser.add_argument("--servers", type=int, default=10,
                        help="how many servers to listen to (default 10)")
    parser.add_argument("--watch", default="",
                        help="which transactions to print (default: all)")
    parser.add_argument("--quiet", action="store_true",
                        help="do not print the periodic status line")
    options = parser.parse_args()

    watching = parse_what_to_watch(options.watch)

    def print_transaction(details: dict) -> None:
        print(json.dumps(details, default=str), flush=True)

    feed = TransactionFeed(on_transaction=print_transaction, only_types=watching)

    print(f"listening to {options.servers} servers; "
          f"watching {', '.join(watching) if watching else 'everything'}",
          file=sys.stderr)

    try:
        feed.start(number_of_servers=options.servers)
    except RuntimeError as problem:
        sys.exit(str(problem))

    try:
        last_status = time.time()
        while not feed.wait(timeout=1):
            if options.quiet or time.time() - last_status < STATUS_LINE_EVERY_SECONDS:
                continue
            counts = feed.counts
            print(f"[{len(feed.connected_servers)} servers connected] "
                  f"printed={counts['reported']} "
                  f"repeats_ignored={counts['repeats_ignored']} "
                  f"unreadable={counts['unreadable']} "
                  f"compressed_skipped={counts['compressed_skipped']} "
                  f"dropped={counts['dropped_backlog']} "
                  f"refused={counts['connection_failures']}",
                  file=sys.stderr)
            last_status = time.time()
    except KeyboardInterrupt:
        feed.stop()
        print("stopped", file=sys.stderr)
        return

    if isinstance(feed.failure, BrokenPipeError):
        # Whoever was reading our output has gone, e.g. `| head`. Leave quietly;
        # pointing stdout at /dev/null stops the final flush from complaining.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return
    if feed.failure is not None:
        sys.exit(f"stopped: {feed.failure!r}")


if __name__ == "__main__":
    main()
