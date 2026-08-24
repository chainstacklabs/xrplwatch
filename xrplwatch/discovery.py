"""Finding XRP Ledger servers you can connect to.

You do not need a directory service. Any public server will tell you about the
other servers it is currently connected to, and you can ask any of those for
their neighbours in turn. This is how the network is designed to be joined.

Which servers you end up with matters as much as how many, because a transaction
reaches nearby servers first. This module does not try to be clever about that -
it returns what the network offers, in the order offered. Watch the `_heard_from`
field to see which ones answer first, then feed those back in with `addresses=`.

The list comes from a plain web address on the server's own port, described here:
https://xrpl.org/docs/references/http-websocket-apis/peer-port-methods/peer-crawler
"""

import ipaddress
import json
import ssl
import urllib.request

# Well-known public servers that are connected to a lot of others, so their lists
# are a good starting point. More than one, because a single seed being down or
# unfriendly should not leave us with nothing.
STARTING_POINTS = (
    "https://s1.ripple.com:51235/crawl",
    "https://s2.ripple.com:51235/crawl",
)

REQUEST_PATIENCE_SECONDS = 25


def _relaxed_certificate_check() -> ssl.SSLContext:
    """XRP Ledger servers use self-signed certificates, so skip the usual check.

    This only affects reading the public server list, which contains no secrets
    and which we do not act on blindly - every address is verified by actually
    connecting and completing the signed greeting.
    """
    settings = ssl.create_default_context()
    settings.check_hostname = False
    settings.verify_mode = ssl.CERT_NONE
    return settings


def _public_ipv4(address: str) -> str | None:
    """The address as plain dotted IPv4 if it is worth dialling, else None.

    Crawl listings do sometimes carry private or loopback addresses, and dialling
    those is at best a wasted timeout and at worst a connection to something on
    your own network. They also write some IPv4 peers in IPv6 clothing, as
    `::ffff:1.2.3.4`; those are unwrapped. Real IPv6 is skipped because this
    project does not dial it.
    """
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return None
    if parsed.version == 6:
        parsed = parsed.ipv4_mapped
        if parsed is None:
            return None
    if (parsed.is_private or parsed.is_loopback or parsed.is_link_local
            or parsed.is_multicast or parsed.is_reserved or parsed.is_unspecified):
        return None
    return str(parsed)


def _servers_listed_by(starting_point: str) -> list[tuple[str, int]]:
    request = urllib.request.Request(starting_point)
    with urllib.request.urlopen(
        request, timeout=REQUEST_PATIENCE_SECONDS, context=_relaxed_certificate_check()
    ) as response:
        listing = json.loads(response.read())

    found = []
    for entry in listing.get("overlay", {}).get("active", []):
        address = _public_ipv4(str(entry.get("ip", "")))
        port = entry.get("port")
        if address and port:
            found.append((address, int(port)))
    return found


def find_servers(how_many: int, starting_points=STARTING_POINTS) -> list[tuple[str, int]]:
    """Ask public servers which other servers they are talking to.

    Returns a list of (address, port), in the order the network offered them,
    without duplicates. Expect many of them to refuse you - busy servers answer
    "503 Service Unavailable" when they already have enough connections - so ask
    for more than you need.

    Raises RuntimeError only if every starting point failed.
    """
    found: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    problems = []

    for starting_point in starting_points:
        try:
            listed = _servers_listed_by(starting_point)
        except (OSError, ValueError) as problem:     # unreachable, or not JSON we expect
            problems.append(f"{starting_point}: {problem}")
            continue
        for server in listed:
            if server not in seen:
                seen.add(server)
                found.append(server)
        if len(found) >= how_many:
            break

    if not found:
        raise RuntimeError("No starting point would list any servers: "
                           + "; ".join(problems))

    return found[:how_many]
