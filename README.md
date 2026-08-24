<img width="1200" alt="Labs" src="https://user-images.githubusercontent.com/99700157/213291931-5a822628-5b8a-4768-980d-65f324985d32.png">

<p>
 <h3 align="center">Chainstack is the leading suite of services connecting developers with Web3 infrastructure</h3>
</p>

<p align="center">
  • <a target="_blank" href="https://chainstack.com/">Homepage</a> •
  <a target="_blank" href="https://chainstack.com/protocols/">Supported protocols</a> •
  <a target="_blank" href="https://chainstack.com/blog/">Chainstack blog</a> •
  <a target="_blank" href="https://docs.chainstack.com/reference/">Blockchain API reference</a> • <br> 
  • <a target="_blank" href="https://console.chainstack.com/user/account/create">Start for free</a> •
</p>

# xrplwatch

Joins the XRP Ledger [peer-to-peer network](https://xrpl.org/docs/concepts/networks-and-servers/peer-protocol)
as a peer and reads transactions off it, instead of asking a server what it knows.
A worked example of reading the gossip layer, where a transaction first appears.

## How the XRP Ledger works

- A new [ledger](https://xrpl.org/docs/concepts/ledgers) closes every ~4 seconds.
  That is the clock everything runs on.
- There is **no mempool** in the Ethereum sense. A transaction is submitted to one
  server, relayed to that server's peers, and applied to the *open* ledger each server
  is assembling. Ones that do not fit go to a
  [queue](https://xrpl.org/docs/concepts/transactions/transaction-queue) for a later
  ledger.
- [Consensus](https://xrpl.org/docs/concepts/consensus-protocol) picks the
  transaction set for the next ledger, then applies a
  [canonical order](https://xrpl.org/docs/concepts/consensus-protocol/consensus-structure)
  to it. Order inside a ledger is **not** arrival order.
- Once a ledger is validated it is final — no reorgs, no confirmation counting.
  See [Finality of Results](https://xrpl.org/docs/concepts/transactions/finality-of-results).

## The two clients you will meet

[**rippled**](https://github.com/XRPLF/rippled) is the node. It joins the peer
network, takes part in consensus, holds the open ledger, and sees transactions the
moment a peer relays them — including ones not yet in any ledger. It keeps a
[rolling window of history](https://xrpl.org/docs/concepts/networks-and-servers/ledger-history),
not the whole chain.

[**Clio**](https://xrpl.org/docs/concepts/networks-and-servers/the-clio-server) is
not a node. It is a read-only API server that extracts validated data from a rippled
into its own store, so heavy historical queries do not load the node. It serves
validated data and forwards anything live to a rippled behind it, which costs a hop.

Tell them apart with `server_info`: Clio reports a `clio_version`. Public endpoints
differ — in August 2026, `s1`/`s2.ripple.com` ran Clio 2.8.0 while `xrplcluster.com`
and `xrpl.ws` ran rippled 3.3.0. If you are chasing latency, prefer rippled.

## Where the peer list comes from

There is no directory service. Every rippled exposes a
[`/crawl`](https://xrpl.org/docs/references/http-websocket-apis/peer-port-methods/peer-crawler)
endpoint on its peer port that lists the peers it is currently connected to, so one
public server is enough to bootstrap into the network.

We ask `s1.ripple.com:51235/crawl` and `s2.ripple.com:51235/crawl`, merge and
deduplicate the results, drop private and IPv6 addresses, and dial what is left.
Every address is then verified by actually connecting and completing the signed
greeting — the crawl is a hint, not a trust anchor.

The peers you end up with are whoever had a free slot, not the best-placed ones.
`_heard_from` on each transaction tells you which ones answer first; pass those back
in with `feed.start(addresses=[...])`.

## How it works

Once there are addresses to try, each gets its own thread doing the same four things.

**Connect.** Open TLS to the server's peer port — 2459, or 51235 on older servers,
the same port servers use with each other. Send an HTTP-style greeting with
`Connect-As: Peer`, offering protocol versions `XRPL/2.3`, `2.2` and `2.1`. The server
either answers `101 Switching Protocols` and starts talking, or refuses because it is
already full.

**Prove who we are.** The greeting carries a public key and a signature. What gets
signed is a value derived from the TLS Finished messages both sides exchanged, so the
signature is meaningless anywhere except that one connection and useless to anyone who
copies it. Both ends can compute the value; nobody else can. This mirrors
`Handshake.cpp` in rippled.

**Read the stream.** After the upgrade the server sends everything it hears about, as
binary messages packed back to back. Each message begins with a header giving its
length and type, and its body is a series of numbered fields. Two types carry
transactions: 30 for a single one, 64 for a batch.

**Name it and drop repeats.** Hashing the raw transaction gives the same id every
explorer shows, without parsing any of it:

```python
hashlib.sha512(b"TXN\x00" + raw).digest()[:32].hex().upper()
```

Every peer relays the same traffic, so most of what arrives has been seen already. The
first mention of an id is reported and the rest are counted and discarded. Decoding
into readable fields happens on a separate thread, so working out what a transaction
says never holds up reading the next message.

## Run it

```bash
uv run python xrpl_feed.py --watch trades
uv run python xrpl_feed.py --watch OfferCreate,Payment --servers 12
```

One JSON transaction per line, plus `_id`, `_heard_from` and `_heard_at`. Groups for
`--watch`: `trades`, `pool-trades`, `new-pools`, `payments`, `new-tokens`,
`collectibles`, or any
[official type name](https://xrpl.org/docs/references/protocol/transactions/types).

From your own code:

```python
from xrplwatch import TransactionFeed

feed = TransactionFeed(on_transaction=print, only_types=["OfferCreate"])
feed.start(number_of_servers=10)
```

Allow a minute to warm up: most servers are full and refuse new peers. In one 30
minute run, 67 addresses had to be dialled to hold 28 connections.

To check everything still works, from parsing to the live network:

```bash
./smoke.sh            # tests, then finds servers, connects, reads transactions
./smoke.sh --offline  # tests only
```

## Limitations

- **Peer slots are scarce.** The same 30 minute run logged 1,355 refusals while
  holding those 28 connections. Nothing entitles you to a slot on someone else's
  server.
- **Unvalidated data.** A transaction you see may never make it into a ledger, and its
  final order is decided at consensus. Confirm against a validated ledger before
  treating anything as true.
- **Transactions only.** Consensus and ledger-data messages are ignored, compressed
  messages are skipped, and IPv6 peers are not dialled.
- **The head start is small.** Measured once from a single location: seconds ahead of
  validated-ledger data, but only tens of milliseconds ahead of a WebSocket
  subscription to a well-connected rippled. Indicative, not a benchmark.

## Where this goes next

The step after reading the network from other people's servers is a node of your
own — either [self-hosted](https://docs.chainstack.com/docs/self-hosted), or from an
RPC provider such as [Chainstack](https://chainstack.com/build-better-with-xrp-ledger/).
A node you control gives you a peer slot nobody can refuse you, the same data over an
ordinary WebSocket subscription, and a place to submit transactions from.

If you want to take this code further, the things that would actually move money:

- **Pick peers by measured first-sighting rate, not by who answered.** This is the
  dominant factor by a wide margin — in one run a single peer was first for close to
  half of all transactions while others were first for a handful. Rank them from
  `_heard_from` and keep the winners.
- **Peer near where transactions originate.** Proximity to active submitters beats
  proximity to you, and beats peer count.
- **Track ledger close timing.** Consensus and status messages tell you where you are
  in the ~4 second window. That is what decides whether a transaction you send lands
  in the current ledger or the next one, which is a far bigger difference than any
  detection latency.
- **Spend the effort on the submission path.** Detection is close to its ceiling.
  Being tens of milliseconds earlier to *see* a trade is worthless if submitting takes
  hundreds, so measure the route out as carefully as the route in.
- **Filter on raw bytes before decoding.** Decoding costs a few hundred microseconds
  per transaction; matching the type and the assets you care about on the wire format
  first keeps a burst from queueing behind work you did not need.
