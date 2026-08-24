"""xrplwatch - listen to the XRP Ledger peer network directly.

XRP Ledger servers gossip transactions to each other over their own peer-to-peer
protocol. This package joins that network as a peer and reads the gossip, rather
than asking one server what it knows.

    from xrplwatch import TransactionFeed

    feed = TransactionFeed(on_transaction=print, only_types=["OfferCreate"])
    feed.start(number_of_servers=10)

The modules, in the order they are worth reading:

    identity      making a name for ourselves and proving it
    connection    opening a connection to one server
    wire          turning raw bytes into messages
    transactions  finding transactions in messages and decoding them
    discovery     finding servers to connect to
    feed          listening to many servers at once
"""

from . import connection, discovery, identity, transactions, wire
from .feed import TransactionFeed

__all__ = [
    "TransactionFeed",
    "connection",
    "discovery",
    "identity",
    "transactions",
    "wire",
]
