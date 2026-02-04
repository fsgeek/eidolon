"""Network model for VMTP simulation.

Abstract network with configurable delay and loss.
Uses SimPy for discrete-event timing.
"""

import simpy
import random
from dataclasses import dataclass, field
from typing import Dict, Callable, Optional, Any
from packet import Packet, Request, Response


@dataclass
class NetworkConfig:
    """Configuration for network behavior."""

    base_delay: float = 0.010  # One-way latency in seconds (10ms default)
    delay_jitter: float = 0.002  # Random jitter added to delay
    loss_probability: float = 0.0  # Probability of dropping a packet
    seed: Optional[int] = None  # Random seed for reproducibility

    def __post_init__(self):
        if self.seed is not None:
            random.seed(self.seed)


class Network:
    """Simulated network that delivers packets between entities.

    Each entity has a mailbox (SimPy Store) where packets arrive.
    The network handles delay and loss simulation.
    """

    def __init__(self, env: simpy.Environment, config: NetworkConfig = None):
        self.env = env
        self.config = config or NetworkConfig()
        self._mailboxes: Dict[int, simpy.Store] = {}
        self._packet_log: list = []  # Log of all packets for analysis
        self._stats = {
            "sent": 0,
            "delivered": 0,
            "dropped": 0,
        }

    def register(self, entity_id: int) -> simpy.Store:
        """Register an entity and return its mailbox."""
        if entity_id not in self._mailboxes:
            self._mailboxes[entity_id] = simpy.Store(self.env)
        return self._mailboxes[entity_id]

    def get_mailbox(self, entity_id: int) -> Optional[simpy.Store]:
        """Get the mailbox for an entity."""
        return self._mailboxes.get(entity_id)

    def send(self, packet: Packet, destination_id: int) -> simpy.Process:
        """Send a packet to a destination entity.

        Returns a SimPy process that completes when the packet
        is delivered (or dropped).
        """
        return self.env.process(self._deliver(packet, destination_id))

    def _deliver(self, packet: Packet, destination_id: int):
        """Internal delivery process with delay and loss simulation."""
        self._stats["sent"] += 1

        # Log the send
        self._packet_log.append({
            "time": self.env.now,
            "event": "send",
            "packet": packet,
            "destination": destination_id,
        })

        # Check for loss
        if random.random() < self.config.loss_probability:
            self._stats["dropped"] += 1
            self._packet_log.append({
                "time": self.env.now,
                "event": "drop",
                "packet": packet,
                "destination": destination_id,
            })
            return  # Packet is lost, nothing delivered

        # Simulate network delay
        delay = self.config.base_delay
        if self.config.delay_jitter > 0:
            delay += random.uniform(-self.config.delay_jitter, self.config.delay_jitter)
        delay = max(0.001, delay)  # Minimum 1ms delay

        yield self.env.timeout(delay)

        # Deliver to mailbox
        mailbox = self._mailboxes.get(destination_id)
        if mailbox is not None:
            self._stats["delivered"] += 1
            self._packet_log.append({
                "time": self.env.now,
                "event": "deliver",
                "packet": packet,
                "destination": destination_id,
            })
            yield mailbox.put(packet)
        else:
            # Destination doesn't exist - packet lost
            self._stats["dropped"] += 1
            self._packet_log.append({
                "time": self.env.now,
                "event": "no_destination",
                "packet": packet,
                "destination": destination_id,
            })

    @property
    def stats(self) -> dict:
        """Return network statistics."""
        return self._stats.copy()

    @property
    def packet_log(self) -> list:
        """Return the full packet log for analysis."""
        return self._packet_log
