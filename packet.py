"""VMTP packet structures for simulation.

Minimal representation focused on transaction semantics rather than wire format.
"""

from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum, auto


class PacketType(Enum):
    REQUEST = auto()
    RESPONSE = auto()


@dataclass
class Request:
    """A VMTP request packet."""

    client_id: int  # Entity ID of the client
    server_id: int  # Entity ID of the server
    transaction_id: int  # 32-bit transaction identifier
    idempotent: bool  # Can this operation be safely re-executed?
    payload: Any = None  # Application data
    timestamp: float = 0.0  # Simulation time when sent
    retransmit_count: int = 0  # How many times this has been retransmitted

    @property
    def packet_type(self) -> PacketType:
        return PacketType.REQUEST

    def __repr__(self):
        idem = "idem" if self.idempotent else "non-idem"
        return f"Request(client={self.client_id}, txn={self.transaction_id}, {idem})"


@dataclass
class Response:
    """A VMTP response packet."""

    client_id: int  # Entity ID of the client (destination)
    server_id: int  # Entity ID of the server (source)
    transaction_id: int  # Matches the request's transaction ID
    payload: Any = None  # Application response data
    timestamp: float = 0.0  # Simulation time when sent
    is_cached: bool = False  # Was this resent from CSR cache?

    @property
    def packet_type(self) -> PacketType:
        return PacketType.RESPONSE

    def __repr__(self):
        cached = " (cached)" if self.is_cached else ""
        return f"Response(server={self.server_id}, txn={self.transaction_id}{cached})"


# Type alias for any packet
Packet = Request | Response
