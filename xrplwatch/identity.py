"""Creating an identity, and proving it to an XRP Ledger server.

Before a server will talk to you, it wants two things:

  1. A name. Every server on the network has a key pair - a secret half it keeps
     and a public half it shows people. The public half, written in a readable
     form, is its name.

  2. Proof you actually hold the secret half. The server does not want a plain
     signature, because someone could copy it and reuse it elsewhere. Instead it
     asks you to sign something unique to *this exact encrypted connection*, so
     the proof is worthless anywhere else.

The thing you sign is built from the last handshake message each side sent while
setting up encryption. Mix the two together, and both sides can compute the same
value - but only the two of them can, and only for this one connection.

This mirrors what the official server does in `Handshake.cpp` in the rippled
source code: https://github.com/XRPLF/rippled
"""

import hashlib

import coincurve

# The alphabet the XRP Ledger uses to write keys and addresses as text. It leaves
# out characters that are easy to confuse, like 0 and O.
READABLE_ALPHABET = "rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz"

# A marker byte that means "what follows is a server's public key". It is why
# server names always start with the letter n.
SERVER_KEY_MARKER = b"\x1c"


def create_identity() -> coincurve.PrivateKey:
    """Make a fresh key pair to introduce ourselves with.

    We are only listening, never voting on anything, so a throwaway identity per
    connection is fine and avoids looking like one machine opening many links.
    """
    return coincurve.PrivateKey()


def _to_readable_text(payload: bytes) -> str:
    """Write bytes in the XRP Ledger's readable form, with a typo check attached."""
    typo_check = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    full = payload + typo_check

    number = int.from_bytes(full, "big")
    text = ""
    while number:
        number, remainder = divmod(number, 58)
        text = READABLE_ALPHABET[remainder] + text

    leading_zero_bytes = len(full) - len(full.lstrip(b"\0"))
    return READABLE_ALPHABET[0] * leading_zero_bytes + text


def public_name(identity: coincurve.PrivateKey) -> str:
    """The public half of our identity, written the way servers expect."""
    public_half = identity.public_key.format(compressed=True)
    return _to_readable_text(SERVER_KEY_MARKER + public_half)


def value_unique_to_connection(encrypted_connection) -> bytes:
    """Build the value that proves we are on this specific connection.

    Take the last handshake message we sent and the last one we received, scramble
    each, then combine them. Both sides get the same answer; nobody else can.
    """
    ours = hashlib.sha512(encrypted_connection.get_finished()).digest()
    theirs = hashlib.sha512(encrypted_connection.get_peer_finished()).digest()

    if ours == theirs:
        raise RuntimeError(
            "Both handshake messages were identical, which should never happen. "
            "Refusing to continue rather than send a meaningless proof."
        )

    combined = bytes(a ^ b for a, b in zip(ours, theirs))
    return hashlib.sha512(combined).digest()[:32]


def sign_proof(identity: coincurve.PrivateKey, value: bytes) -> bytes:
    """Sign the connection value with the secret half of our identity.

    `hasher=None` matters: the value is already scrambled, and the server expects
    a signature over it exactly as-is rather than over a scramble of it.
    """
    return identity.sign(value, hasher=None)
