"""Finding transactions inside messages, and reading what they say.

A transaction is one action somebody took: sending money, placing a trade,
creating a token, and so on. Servers pass them around as compact bytes.

Two useful things you can do with those bytes:

  * Give the transaction a name, without understanding it at all. Scramble the
    bytes in the standard way and you get its id - the same id every server and
    explorer uses. This is enough to notice "I have seen this one already".

  * Read what it actually says, using the official xrpl-py library.

Full list of what transactions can be:
https://xrpl.org/docs/references/protocol/transactions/types
"""

import hashlib
from collections.abc import Iterator

from xrpl.core.binarycodec import decode

from . import wire

# Prepended before scrambling so a transaction id can never collide with the id
# of some other kind of object that happens to have the same bytes.
TRANSACTION_MARKER = b"TXN\x00"

# Message kinds that carry transactions.
ONE_TRANSACTION = 30
SEVERAL_TRANSACTIONS = 64


def transaction_id(raw_transaction: bytes) -> str:
    """The standard id for a transaction, worked out from its bytes alone.

    You can compute this the moment bytes arrive, before understanding any of
    them, which is what makes deduplicating across many servers cheap.
    """
    scrambled = hashlib.sha512(TRANSACTION_MARKER + raw_transaction).digest()
    return scrambled[:32].hex().upper()


def _chunks_in_field_one(data: bytes) -> Iterator[bytes]:
    """Every field-1 value in `data` that is a chunk of bytes."""
    for field_number, value_kind, value in wire.read_fields(data):
        if field_number == 1 and value_kind == 2 and isinstance(value, bytes):
            yield value


def transactions_in_message(kind: int, content: bytes) -> Iterator[bytes]:
    """Pull the raw transaction bytes out of a message.

    Servers send transactions either one at a time or in small batches, so both
    shapes are handled here. Yields raw bytes, one per transaction.
    """
    if kind == ONE_TRANSACTION:
        # The transaction itself is stored in field number 1.
        yield from _chunks_in_field_one(content)

    elif kind == SEVERAL_TRANSACTIONS:
        # Field 1 repeats, and each repeat is a whole one-transaction message,
        # so we look inside each of them for its own field 1.
        for inner_message in _chunks_in_field_one(content):
            yield from _chunks_in_field_one(inner_message)


def read_transaction(raw_transaction: bytes) -> dict:
    """Turn transaction bytes into a plain dictionary you can inspect.

    The result has keys like `TransactionType`, `Account`, and `Fee`. Raises if
    the bytes are not a transaction we can read.
    """
    return decode(raw_transaction.hex().upper())


# Handy groupings, so you do not have to memorise the official names.
WHAT_PEOPLE_USUALLY_WATCH = {
    "trades": ["OfferCreate", "OfferCancel"],
    "pool-trades": ["AMMDeposit", "AMMWithdraw", "AMMBid"],
    "new-pools": ["AMMCreate"],
    "payments": ["Payment"],
    "new-tokens": ["TrustSet"],
    "collectibles": ["NFTokenMint", "NFTokenCreateOffer", "NFTokenAcceptOffer"],
}


def names_for(group_or_name: str) -> list[str]:
    """Accept either a friendly group name or an official transaction name."""
    return WHAT_PEOPLE_USUALLY_WATCH.get(group_or_name, [group_or_name])
