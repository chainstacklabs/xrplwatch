"""Turning raw bytes from the network into something usable.

XRP Ledger servers talk to each other in a compact binary format instead of text
like JSON. This file has the readers you need to understand it. Nothing here
knows anything about payments or trading - it is purely "bytes in, numbers and
chunks out".

There are two layers:

  1. Messages arrive back to back on one long stream of bytes, so each one starts
     with a small header saying "the next N bytes belong to me, and I am a message
     of type T". `split_into_messages` handles that.

  2. Inside a message, the content is stored as numbered fields. `read_fields`
     walks through them.

Two things here are deliberate rather than incidental:

  * We wait for bytes inside `select`, not in a spin loop. A dozen reader threads
    each spinning would burn a dozen cores for nothing.
  * We never slice the front off the buffer per message. That turns a busy
    connection into quadratic copying.

Official description of the server-to-server protocol:
https://xrpl.org/docs/concepts/networks-and-servers/peer-protocol
"""

import select
import struct
import time

from OpenSSL import SSL

# Messages say what kind they are with a number. The ones this project acts on are
# named where they are used: 30 and 64 in transactions.py, 3 in connection.py, 55
# in feed.py. The full list lives in the official xrpl.proto.

# A normal message header is 6 bytes: 4 for the size, 2 for the kind.
PLAIN_HEADER_SIZE = 6
# A compressed message header is 10 bytes, because it also records the size the
# content will have once unpacked.
COMPRESSED_HEADER_SIZE = 10

# If a header claims a message bigger than this, we have lost our place in the
# stream rather than found a genuinely enormous message.
LARGEST_SENSIBLE_MESSAGE = 64 * 1024 * 1024

RECEIVE_CHUNK_SIZE = 1 << 16

# Reclaim the consumed front of the buffer once this much has piled up. Doing it
# per message would be quadratic; never doing it would grow without bound.
COMPACT_BUFFER_AFTER_BYTES = 1 << 20

# Longest we sit in select before checking the clock again.
WAIT_SLICE_SECONDS = 1.0


def read_whole_number(data, position: int) -> tuple[int, int]:
    """Read one number that was stored using a variable number of bytes.

    Small numbers take one byte, bigger numbers take more. Each byte carries
    7 bits of the number; the top bit means "there is another byte after me".

    Returns the number, and the position just after it.
    """
    number = 0
    shift = 0
    while True:
        byte = data[position]
        position += 1
        number = number | ((byte & 0x7F) << shift)
        if not byte & 0x80:
            return number, position
        shift += 7


def write_whole_number(number: int) -> bytes:
    """The reverse of `read_whole_number`, for the few things we send."""
    out = bytearray()
    while True:
        seven_bits = number & 0x7F
        number >>= 7
        out.append(seven_bits | (0x80 if number else 0))
        if not number:
            return bytes(out)


def read_fields(data):
    """Walk through the numbered fields inside a message.

    Yields one tuple per field: (field_number, kind_of_value, value).

    `kind_of_value` is 2 when the value is a chunk of bytes (which is the only
    case this project reads in anger) and 0 when it is a plain number.

    Stops early, without raising, if the data is cut off mid-field or uses a kind
    of value this project does not understand.
    """
    position = 0
    length = len(data)
    try:
        while position < length:
            label, position = read_whole_number(data, position)
            field_number = label >> 3
            kind_of_value = label & 7

            if kind_of_value == 0:                       # a plain number
                value, position = read_whole_number(data, position)
                yield field_number, kind_of_value, value
            elif kind_of_value == 2:                     # a chunk of bytes
                size, position = read_whole_number(data, position)
                yield field_number, kind_of_value, bytes(data[position:position + size])
                position += size
            elif kind_of_value == 5:                     # fixed 4 bytes
                yield field_number, kind_of_value, bytes(data[position:position + 4])
                position += 4
            elif kind_of_value == 1:                     # fixed 8 bytes
                yield field_number, kind_of_value, bytes(data[position:position + 8])
                position += 8
            else:
                return
    except IndexError:
        return                                           # cut off mid-number


def build_message(kind: int, content: bytes) -> bytes:
    """Wrap content in the 6-byte header so a server will accept it."""
    return struct.pack(">IH", len(content), kind) + content


def header_length(first_byte: int) -> int | None:
    """How long this message's header is, judging by its first byte.

    Returns None when the byte cannot begin either kind of header, which means
    we have lost track of where messages start.
    """
    # A set top bit means the sender compressed this message.
    if first_byte & 0x80:
        return COMPRESSED_HEADER_SIZE
    # Six zero bits at the front means a normal, uncompressed message.
    if (first_byte & 0xFC) == 0:
        return PLAIN_HEADER_SIZE
    return None


def split_into_messages(connection, leftover: bytes, stop_at: float):
    """Read from a connection and hand back one message at a time.

    Yields (arrival_time, kind, content). `content` is None for compressed
    messages, which this project skips. The caller counts those, so if servers
    ever start compressing it shows up as a number rather than as mysteriously
    fewer transactions.

    `arrival_time` is when the bytes that completed the message were read, not
    when this generator got round to parsing them. Several messages delivered in
    one chunk therefore share a timestamp, which is the truth of the matter.

    Stops when the clock passes `stop_at`, when the other side hangs up, or when
    the stream stops making sense.
    """
    buffered = bytearray(leftover)
    consumed = 0
    arrived_at = time.time()          # the leftover rode in with the greeting

    def pull_more() -> bool:
        """Wait for more bytes and add them. False when the link is finished."""
        nonlocal arrived_at
        patience = min(WAIT_SLICE_SECONDS, max(0.0, stop_at - time.time()))
        try:
            chunk = connection.recv(RECEIVE_CHUNK_SIZE)
        except SSL.WantReadError:
            select.select([connection], [], [], patience)
            return True
        except SSL.WantWriteError:
            select.select([], [connection], [], patience)
            return True
        except (SSL.ZeroReturnError, SSL.SysCallError, SSL.Error, OSError):
            return False              # gone, as opposed to "nothing yet"
        if not chunk:
            return False
        arrived_at = time.time()
        buffered.extend(chunk)
        return True

    while time.time() < stop_at:
        if consumed > COMPACT_BUFFER_AFTER_BYTES:
            del buffered[:consumed]
            consumed = 0

        if len(buffered) - consumed < PLAIN_HEADER_SIZE:
            if not pull_more():
                return
            continue

        header_size = header_length(buffered[consumed])
        if header_size is None:
            return                    # lost our place; the caller reconnects

        if len(buffered) - consumed < header_size:
            if not pull_more():
                return
            continue

        content_size = struct.unpack_from(">I", buffered, consumed)[0]
        kind = struct.unpack_from(">H", buffered, consumed + 4)[0]
        if header_size == COMPRESSED_HEADER_SIZE:
            content_size &= 0x0FFFFFFF
        if content_size > LARGEST_SENSIBLE_MESSAGE:
            return                    # a size that large means we are misreading

        whole_message = header_size + content_size
        while len(buffered) - consumed < whole_message:
            if time.time() >= stop_at or not pull_more():
                return

        body_starts = consumed + header_size
        content = bytes(buffered[body_starts:consumed + whole_message])
        consumed += whole_message

        was_compressed = header_size == COMPRESSED_HEADER_SIZE
        yield arrived_at, kind, (None if was_compressed else content)
