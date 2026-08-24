"""The tests that would catch a real breakage, and no others.

Everything here guards a failure that does not announce itself. The protocol has
no error for "you are parsing me wrong" - you just quietly stop seeing
transactions, which looks exactly like a quiet network. So:

  * the ids and decoding are checked against two real mainnet transactions, so the
    code has to agree with the network rather than with itself
  * the framing is checked against awkward chunk boundaries, a corrupt stream and
    a dead socket, because those are the ways a feed silently stalls
  * the ping reply is checked, because rippled matches answers to questions by
    their sequence number and a malformed reply is not reported back to you
  * a callback that raises has to stop the feed, because a dead decoder thread
    otherwise looks exactly like a quiet network

No network needed. Run with ./smoke.sh --offline
"""

import struct
import threading
import time

import pytest
from OpenSSL import SSL

from xrplwatch import connection, discovery, transactions, wire
from xrplwatch.feed import TransactionFeed

# Two real transactions from XRP Ledger mainnet, with the ids the network gave
# them. Captured from ledger 106508780 via a `tx` request with binary: true.
CANCEL_ID = "033BF053AADEE7FF1A2CD8A0FDD5243FFBC2C64B3055B0FC4CB037AB04ABA75E"
CANCEL = bytes.fromhex(
    "120008240619EC5820190619EC5568400000000000000A7321ED9BA85E762FC148FF29F239FDB7EA0069405878"
    "85CD2628B786BC5051BBF61CF3744056F27C4E7A706C34C62F9CC0C95E4D5398BAB2CFD9F29D1779887DF77609"
    "99529CE7C285A97DA69511A22B0D215FC3F29CEAAC16388C5A4B817B9E20BE587D03811419756667501ED2C34B"
    "69B7C244935924C63D09E9")

CREATE_ID = "056C31E27EEA1C87D5B5D7D3CED52D0BCFF56D1E3FCDFA76BF37ED1C8CBBEC18"
CREATE = bytes.fromhex(
    "1200072200000000240A0E7414201B065931F264D54C900EC39F85EE00000000000000000000000045555200"
    "000000002ADB0B3959D60A6E6991F729E1918B716392523065D4D9F85FB126FB1000000000000000000000000"
    "04C5443000000000006B36AC50AC7331069070C6350B202EAF2125C7C68400000000000000A732103C48299E5"
    "7F5AE7C2BE1391B581D313F1967EA2301628C07AC412092FDC15BA2274473045022100A6B69427FB40AEE99FF"
    "4A55639434CEC5AB468B80B21F40E79CA3F1018769B0502203FE9F92F7219DE0BA0DF8EC0108EF25122EC2926"
    "873749817BFF0308F6DCCE5C81144CCBCFB6AC83679498E02B0F6B36BE537CB422E5")

REAL_TRANSACTIONS = [(CANCEL_ID, CANCEL, "OfferCancel"), (CREATE_ID, CREATE, "OfferCreate")]


class FakeLink:
    """Hands out prepared chunks, then reports the connection finished."""

    def __init__(self, chunks, fail_with=None):
        self.chunks = list(chunks)
        self.fail_with = fail_with
        self.sent = []
        self.reads = 0

    def recv(self, _how_many):
        self.reads += 1
        if self.chunks:
            return self.chunks.pop(0)
        if self.fail_with:
            raise self.fail_with
        return b""

    def sendall(self, data):
        self.sent.append(data)


def field_one(payload: bytes) -> bytes:
    return b"\x0a" + wire.write_whole_number(len(payload)) + payload


def one_transaction(blob: bytes) -> bytes:
    return wire.build_message(30, field_one(blob))


def batch_of(blobs) -> bytes:
    return wire.build_message(64, b"".join(field_one(field_one(one)) for one in blobs))


def soon() -> float:
    return time.time() + 30


class TestWeAgreeWithTheNetwork:
    """If these fail, the code is wrong about the protocol itself."""

    @pytest.mark.parametrize("expected_id,blob,_kind", REAL_TRANSACTIONS)
    def test_the_id_we_compute_is_the_id_the_network_uses(self, expected_id, blob, _kind):
        assert transactions.transaction_id(blob) == expected_id

    @pytest.mark.parametrize("_id,blob,kind", REAL_TRANSACTIONS)
    def test_a_real_transaction_decodes(self, _id, blob, kind):
        details = transactions.read_transaction(blob)
        assert details["TransactionType"] == kind
        assert details["Account"].startswith("r")


class TestFindingTransactionsInMessages:
    def test_a_single_transaction_message(self):
        found = list(transactions.transactions_in_message(30, field_one(CANCEL)))
        assert found == [CANCEL]

    def test_a_batch_message_yields_every_transaction(self):
        # Batches nest a whole single-transaction message inside each repeat. Get
        # this wrong and you silently lose most of the feed, because busy servers
        # send batches.
        content = batch_of([CANCEL, CREATE])[wire.PLAIN_HEADER_SIZE:]
        found = list(transactions.transactions_in_message(64, content))
        assert found == [CANCEL, CREATE]
        assert [transactions.transaction_id(one) for one in found] == [CANCEL_ID, CREATE_ID]


class TestFramingTheByteStream:
    """The ways a feed stops working without saying so."""

    def test_several_messages_arriving_in_one_chunk(self):
        stream = one_transaction(CANCEL) + one_transaction(CREATE)
        got = list(wire.split_into_messages(FakeLink([stream]), b"", soon()))
        assert len(got) == 2

    def test_one_message_split_across_chunks(self):
        stream = one_transaction(CANCEL)
        pieces = [stream[:3], stream[3:40], stream[40:]]
        got = list(wire.split_into_messages(FakeLink(pieces), b"", soon()))
        assert [content for _, _, content in got] == [field_one(CANCEL)]

    def test_bytes_that_arrived_with_the_greeting_are_not_lost(self):
        got = list(wire.split_into_messages(FakeLink([]), one_transaction(CANCEL), soon()))
        assert len(got) == 1

    def test_a_corrupt_stream_stops_instead_of_guessing(self):
        got = list(wire.split_into_messages(FakeLink([b"\x7f" * 6]), b"", soon()))
        assert got == []

    def test_a_field_cut_off_mid_number_stops_instead_of_crashing(self):
        # A field-1 chunk whose length byte says "more follows", then nothing. An
        # IndexError here would take the reader thread for that server with it.
        assert list(wire.read_fields(b"\x0a\x80")) == []

    def test_a_dead_socket_stops_instead_of_spinning(self):
        # This one spun until its deadline once, which for a live feed was an hour
        # of a busy core producing nothing.
        link = FakeLink([], fail_with=SSL.SysCallError(104, "reset by peer"))
        assert list(wire.split_into_messages(link, b"", soon())) == []
        assert link.reads == 1


class TestChoosingWhomToDial:
    @pytest.mark.parametrize("listed,dialled", [
        ("84.32.97.85", "84.32.97.85"),
        ("::ffff:78.89.200.25", "78.89.200.25"),   # crawl lists some IPv4 peers this way
        ("10.0.0.7", None),
        ("2a01:4f8::1", None),
        ("not-an-address", None),
    ])
    def test_only_public_ipv4_is_dialled_and_mapped_addresses_are_unwrapped(
            self, listed, dialled):
        assert discovery._public_ipv4(listed) == dialled


class TestStayingConnected:
    def test_the_ping_reply_echoes_the_sequence_number(self):
        # rippled matches answers to questions by this number, so it has to come
        # back unchanged.
        link = FakeLink([])
        ping = b"\x08\x00" + b"\x10" + wire.write_whole_number(12345)
        assert connection.answer_ping(link, ping) is True

        body = link.sent[0][wire.PLAIN_HEADER_SIZE:]
        assert link.sent[0][:wire.PLAIN_HEADER_SIZE] == struct.pack(
            ">IH", len(body), connection.ARE_YOU_ALIVE)
        assert {n: v for n, _, v in wire.read_fields(body)} == {1: 1, 2: 12345}

    def test_we_do_not_reply_to_a_reply(self):
        link = FakeLink([])
        assert connection.answer_ping(link, b"\x08\x01\x10\x07") is False
        assert link.sent == []


class TestReportingEachTransactionOnce:
    def test_the_same_transaction_from_three_servers_is_reported_once(self):
        seen = []
        feed = TransactionFeed(on_transaction=seen.append)
        for server in ("a:1", "b:2", "c:3"):
            feed._read_until_dropped(FakeLink([one_transaction(CANCEL)]), b"", server)

        assert feed._waiting.qsize() == 1
        assert feed.counts["repeats_ignored"] == 2

    def test_a_reported_transaction_says_where_and_when_we_heard_it(self):
        seen = []
        feed = TransactionFeed(on_transaction=seen.append)
        feed._read_until_dropped(FakeLink([one_transaction(CANCEL)]), b"", "1.2.3.4:2459")
        feed._report(*feed._waiting.get_nowait())

        assert seen[0]["TransactionType"] == "OfferCancel"
        assert seen[0]["_id"] == CANCEL_ID
        assert seen[0]["_heard_from"] == "1.2.3.4:2459"

    def test_a_callback_that_raises_stops_the_feed_and_says_why(self):
        # Before this, one exception in the callback killed the decoder thread and
        # the feed ran on reporting nothing - `| head` hung, for instance.
        def refuse(_details):
            raise BrokenPipeError

        feed = TransactionFeed(on_transaction=refuse)
        feed._read_until_dropped(FakeLink([one_transaction(CANCEL)]), b"", "a:1")
        threading.Thread(target=feed._decode_forever, daemon=True).start()

        assert feed.wait(timeout=5) is True
        assert isinstance(feed.failure, BrokenPipeError)

    def test_a_compressed_message_is_counted_not_silently_dropped(self):
        # Servers send everything uncompressed today. If that changes, this counter
        # is how you find out, rather than by seeing fewer transactions.
        header = struct.pack(">I", 0x80000000 | 4) + struct.pack(">H", 30) + b"\x00" * 4
        feed = TransactionFeed(on_transaction=lambda _: None)
        feed._read_until_dropped(FakeLink([header + b"body"]), b"", "a:1")
        assert feed.counts["compressed_skipped"] == 1
        assert feed._waiting.qsize() == 0
