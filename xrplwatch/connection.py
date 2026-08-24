"""Opening a connection to an XRP Ledger server.

Two steps:

  1. Set up an encrypted link, the same kind your browser uses for https.
  2. Send a short greeting proving who we are, and check the server agrees to
     upgrade the link from "web request" to "server-to-server conversation".

A successful greeting is answered with `101 Switching Protocols`. After that the
server just starts sending us everything it hears about, and we read messages
until we stop.

Two details worth knowing:

  * We offer `XRPL/2.3`, `2.2` and `2.1`, newest first, rather than pinning one -
    a peer that only speaks one of them is otherwise a listening post thrown away
    for no reason. Servers we connected to agreed on `XRPL/2.2`.
  * `TMPing` carries a sequence number, and rippled uses it to match answers to
    questions, so we echo it back. We did not test what happens if you do not.

Official page about this connection:
https://xrpl.org/docs/concepts/networks-and-servers/peer-protocol
"""

import base64
import select
import socket
import time
from typing import NamedTuple

from OpenSSL import SSL

from . import identity, wire

# Offered newest first; the server picks one and names it in its reply.
CONVERSATION_STYLES = ("XRPL/2.3", "XRPL/2.2", "XRPL/2.1")

# XRP Ledger counts time from the start of the year 2000 rather than 1970.
SECONDS_BETWEEN_1970_AND_2000 = 946684800

HANDSHAKE_PATIENCE_SECONDS = 25
CONNECT_PATIENCE_SECONDS = 20

ARE_YOU_ALIVE = 3
_ASKING = 0                      # TMPing type: 0 asks, 1 answers


class OpenPeer(NamedTuple):
    """A connected server, ready to read from.

    `leftover` is anything the server sent immediately after its reply - pass it
    straight into `wire.split_into_messages` so no early message is lost.
    `style` is the conversation style the server actually agreed to.
    """

    link: SSL.Connection
    leftover: bytes
    style: str


def _wait_for_encryption(encrypted_connection) -> None:
    """Finish setting up encryption, waiting rather than spinning."""
    give_up_at = time.time() + HANDSHAKE_PATIENCE_SECONDS
    while True:
        try:
            encrypted_connection.do_handshake()
            return
        except SSL.WantReadError:
            if time.time() > give_up_at:
                raise RuntimeError("Encryption setup took too long")
            select.select([encrypted_connection], [], [], 0.5)
        except SSL.WantWriteError:
            if time.time() > give_up_at:
                raise RuntimeError("Encryption setup took too long")
            select.select([], [encrypted_connection], [], 0.5)


def _read_reply(encrypted_connection) -> bytes:
    """Read the server's answer to our greeting."""
    received = b""
    give_up_at = time.time() + HANDSHAKE_PATIENCE_SECONDS
    while b"\r\n\r\n" not in received:
        try:
            chunk = encrypted_connection.recv(4096)
        except (SSL.WantReadError, SSL.WantWriteError, socket.timeout):
            if time.time() > give_up_at:
                raise RuntimeError("Server never answered our greeting")
            select.select([encrypted_connection], [], [], 0.5)
            continue
        if not chunk:
            raise RuntimeError("Server hung up during the greeting")
        received += chunk
    return received


def _build_greeting(our_identity, connection_value: bytes, network_number: int,
                    style: str) -> bytes:
    signature = identity.sign_proof(our_identity, connection_value)
    lines = [
        "GET / HTTP/1.1",
        "User-Agent: xrplwatch/0.2",
        f"Upgrade: {style}",
        "Connection: Upgrade",
        "Connect-As: Peer",
        f"Network-ID: {network_number}",
        f"Network-Time: {int(time.time()) - SECONDS_BETWEEN_1970_AND_2000}",
        f"Public-Key: {identity.public_name(our_identity)}",
        f"Session-Signature: {base64.b64encode(signature).decode()}",
        "",
        "",
    ]
    return "\r\n".join(lines).encode()


def _agreed_style(headers: bytes) -> str:
    for line in headers.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"upgrade":
            return value.strip().decode(errors="replace")
    return "unknown"


def _try_one_style(address: str, port: int, our_identity, network_number: int,
                   style: str) -> OpenPeer:
    settings = SSL.Context(SSL.TLS_CLIENT_METHOD)
    # Servers on this network use self-signed certificates, and the official
    # software does not check them either. Security comes from the signed proof
    # below, which is tied to this exact connection and cannot be replayed.
    # Checking certificates here would reject every server on the network.
    #
    # What this does *not* give us: any assurance the peer is honest about what
    # it tells us. Everything it sends is checked on its own merits - a
    # transaction's id comes from its own bytes, not from the peer's word.
    settings.set_verify(SSL.VERIFY_NONE, lambda *_: True)

    plain_socket = socket.create_connection((address, port), timeout=CONNECT_PATIENCE_SECONDS)
    encrypted_connection = SSL.Connection(settings, plain_socket)
    try:
        encrypted_connection.set_tlsext_host_name(address.encode())
        encrypted_connection.set_connect_state()
        _wait_for_encryption(encrypted_connection)

        connection_value = identity.value_unique_to_connection(encrypted_connection)
        encrypted_connection.sendall(
            _build_greeting(our_identity, connection_value, network_number, style)
        )

        reply = _read_reply(encrypted_connection)
        headers, leftover = reply.split(b"\r\n\r\n", 1)
        status_line = headers.split(b"\r\n")[0].decode(errors="replace")

        if "101" not in status_line:
            raise RuntimeError(f"Server refused the connection: {status_line}")

        return OpenPeer(encrypted_connection, leftover, _agreed_style(headers))
    except BaseException:
        # Do not leak the descriptor. With twenty threads reconnecting on a
        # backoff, leaked sockets are what eventually kills a long run.
        try:
            encrypted_connection.close()
        except Exception:
            pass
        plain_socket.close()
        raise


def open_connection(address: str, port: int, our_identity, network_number: int = 0,
                    styles=CONVERSATION_STYLES) -> OpenPeer:
    """Connect to one XRP Ledger server and complete the greeting.

    Tries each conversation style in turn, but only when the refusal was about
    the style itself. A server that is simply full says so, and asking again in
    a different dialect just wastes its time and ours.

    Raises RuntimeError if the server turned us away, OSError if it could not be
    reached at all.
    """
    last_refusal: RuntimeError = RuntimeError("No conversation style was offered")
    for style in styles:
        try:
            return _try_one_style(address, port, our_identity, network_number, style)
        except RuntimeError as refusal:
            last_refusal = refusal
            if "protocol version" not in str(refusal).lower():
                raise
    raise last_refusal


def answer_ping(link, ping_content: bytes) -> bool:
    """Answer a server's "are you alive?", echoing the sequence number it sent.

    rippled uses that number to match answers to questions, so we send it back.

    Returns False when the message was itself an answer rather than a question,
    in which case there is nothing to reply to.
    """
    is_question = True
    echo = bytearray()
    for field_number, value_kind, value in wire.read_fields(ping_content):
        if not isinstance(value, int):
            continue
        if field_number == 1:
            is_question = value == _ASKING
        elif field_number == 2:
            echo += b"\x10" + wire.write_whole_number(value)      # field 2, same number

    if not is_question:
        return False

    reply = b"\x08\x01" + bytes(echo)     # field 1 = 1, meaning "answering"
    link.sendall(wire.build_message(ARE_YOU_ALIVE, reply))
    return True
