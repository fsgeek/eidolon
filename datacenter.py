"""Topology-aware datacenter network for VMTP simulation.

Extends the base Network to support:
- Named locations (datacenters, orbit, moon, mars)
- Per-link latency and loss between locations
- Entity-to-location mapping
- Dynamic topology updates (for orbital mechanics)
- Composable with PartitionableNetwork features
"""

import simpy
import random
from dataclasses import dataclass, field
from typing import Optional

from packet import Packet, Request, Response
from network import Network, NetworkConfig


@dataclass
class Link:
    """A network link between two locations."""
    latency: float  # One-way latency in seconds
    jitter: float = 0.0  # Random jitter added to latency
    loss: float = 0.0  # Loss probability on this link


class DatacenterNetwork(Network):
    """Network with topology-aware routing between named locations.

    Entities are assigned to locations. Packet delay between entities
    depends on which locations they're in - local traffic is fast,
    cross-DC traffic follows the configured link latencies.

    If no link exists between two locations, packets are dropped
    (unreachable).
    """

    def __init__(self, env: simpy.Environment, config: NetworkConfig = None):
        super().__init__(env, config)
        # Location topology
        self._locations: dict[str, set[int]] = {}  # location -> entity IDs
        self._entity_location: dict[int, str] = {}  # entity ID -> location
        self._links: dict[tuple[str, str], Link] = {}  # (loc_a, loc_b) -> Link
        self._default_local_link = Link(latency=0.001, jitter=0.0002)  # 1ms local

        # Partition support (like PartitionableNetwork)
        self._partitioned_locations: set[tuple[str, str]] = set()
        self._partition_drops = 0

    def add_location(self, name: str):
        """Add a named location (datacenter, orbit point, etc)."""
        if name not in self._locations:
            self._locations[name] = set()

    def add_link(self, loc_a: str, loc_b: str, latency: float,
                 jitter: float = 0.0, loss: float = 0.0):
        """Add a bidirectional link between two locations."""
        link = Link(latency=latency, jitter=jitter, loss=loss)
        self._links[(loc_a, loc_b)] = link
        self._links[(loc_b, loc_a)] = link

    def update_link(self, loc_a: str, loc_b: str, latency: float,
                    jitter: Optional[float] = None, loss: Optional[float] = None):
        """Update an existing link's parameters (for dynamic topology)."""
        for key in [(loc_a, loc_b), (loc_b, loc_a)]:
            if key in self._links:
                self._links[key].latency = latency
                if jitter is not None:
                    self._links[key].jitter = jitter
                if loss is not None:
                    self._links[key].loss = loss

    def assign_entity(self, entity_id: int, location: str):
        """Assign an entity to a location."""
        # Remove from old location if any
        old_loc = self._entity_location.get(entity_id)
        if old_loc and old_loc in self._locations:
            self._locations[old_loc].discard(entity_id)

        # Assign to new location
        self.add_location(location)
        self._locations[location].add(entity_id)
        self._entity_location[entity_id] = location

    def migrate_entity(self, entity_id: int, new_location: str):
        """Move an entity to a new location (VMTP migration)."""
        self.assign_entity(entity_id, new_location)

    def get_link(self, src_id: int, dst_id: int) -> Optional[Link]:
        """Get the link between two entities based on their locations."""
        src_loc = self._entity_location.get(src_id)
        dst_loc = self._entity_location.get(dst_id)

        if src_loc is None or dst_loc is None:
            return None

        if src_loc == dst_loc:
            return self._default_local_link

        return self._links.get((src_loc, dst_loc))

    def partition_locations(self, loc_a: str, loc_b: str):
        """Partition two locations (bidirectional)."""
        self._partitioned_locations.add((loc_a, loc_b))
        self._partitioned_locations.add((loc_b, loc_a))

    def heal_locations(self, loc_a: str, loc_b: str):
        """Heal partition between two locations."""
        self._partitioned_locations.discard((loc_a, loc_b))
        self._partitioned_locations.discard((loc_b, loc_a))

    def heal_all(self):
        """Heal all partitions."""
        self._partitioned_locations.clear()

    def _get_source_id(self, packet: Packet) -> Optional[int]:
        """Extract source entity ID from a packet."""
        if isinstance(packet, Request):
            return packet.client_id
        elif isinstance(packet, Response):
            return packet.server_id
        return None

    def _deliver(self, packet: Packet, destination_id: int):
        """Topology-aware delivery with per-link latency."""
        self._stats["sent"] += 1

        source_id = self._get_source_id(packet)

        self._packet_log.append({
            "time": self.env.now,
            "event": "send",
            "packet": packet,
            "destination": destination_id,
        })

        # Check for location-level partition
        if source_id is not None:
            src_loc = self._entity_location.get(source_id)
            dst_loc = self._entity_location.get(destination_id)
            if src_loc and dst_loc and (src_loc, dst_loc) in self._partitioned_locations:
                self._stats["dropped"] += 1
                self._partition_drops += 1
                self._packet_log.append({
                    "time": self.env.now,
                    "event": "partition_drop",
                    "packet": packet,
                    "destination": destination_id,
                })
                return

        # Get link for this entity pair
        link = self.get_link(source_id, destination_id) if source_id else None

        if link is None and source_id is not None:
            # No route between these locations
            self._stats["dropped"] += 1
            self._packet_log.append({
                "time": self.env.now,
                "event": "no_route",
                "packet": packet,
                "destination": destination_id,
            })
            return

        # Check link-level loss
        if link and link.loss > 0 and random.random() < link.loss:
            self._stats["dropped"] += 1
            self._packet_log.append({
                "time": self.env.now,
                "event": "drop",
                "packet": packet,
                "destination": destination_id,
            })
            return

        # Calculate delay from link properties
        if link:
            delay = link.latency
            if link.jitter > 0:
                delay += random.uniform(-link.jitter, link.jitter)
            delay = max(0.0001, delay)  # Minimum 0.1ms
        else:
            # Fallback to base config for unlocated entities
            delay = self.config.base_delay
            if self.config.delay_jitter > 0:
                delay += random.uniform(-self.config.delay_jitter, self.config.delay_jitter)
            delay = max(0.001, delay)

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
            self._stats["dropped"] += 1
            self._packet_log.append({
                "time": self.env.now,
                "event": "no_destination",
                "packet": packet,
                "destination": destination_id,
            })

    @property
    def topology_stats(self) -> dict:
        return {
            **self._stats,
            "locations": len(self._locations),
            "links": len(self._links) // 2,  # bidirectional, so /2
            "entities": len(self._entity_location),
            "partition_drops": self._partition_drops,
        }


# --- Topology presets ---

def single_dc_topology(env: simpy.Environment, seed: int = 42) -> DatacenterNetwork:
    """Single datacenter, all nodes local. ~1ms latency."""
    net = DatacenterNetwork(env, NetworkConfig(seed=seed))
    net.add_location("dc-west")
    return net


def two_dc_topology(env: simpy.Environment, seed: int = 42) -> DatacenterNetwork:
    """Two datacenters: NA-West + Europe. ~100ms inter-DC."""
    net = DatacenterNetwork(env, NetworkConfig(seed=seed))
    net.add_location("na-west")
    net.add_location("europe")
    net.add_link("na-west", "europe", latency=0.050, jitter=0.005)  # 50ms one-way
    return net


def three_dc_topology(env: simpy.Environment, seed: int = 42) -> DatacenterNetwork:
    """Three datacenters: NA-West + Europe + Asia. Heterogeneous latencies."""
    net = DatacenterNetwork(env, NetworkConfig(seed=seed))
    net.add_location("na-west")
    net.add_location("europe")
    net.add_location("asia")
    net.add_link("na-west", "europe", latency=0.050, jitter=0.005)   # 50ms
    net.add_link("na-west", "asia", latency=0.075, jitter=0.008)     # 75ms
    net.add_link("europe", "asia", latency=0.060, jitter=0.006)      # 60ms
    return net


def five_dc_topology(env: simpy.Environment, seed: int = 42) -> DatacenterNetwork:
    """Five datacenters: NA-West + Europe + Asia + SA + Africa."""
    net = DatacenterNetwork(env, NetworkConfig(seed=seed))
    for loc in ["na-west", "europe", "asia", "sa-east", "africa"]:
        net.add_location(loc)
    # Cross-DC links (one-way latencies)
    net.add_link("na-west", "europe", latency=0.050, jitter=0.005)
    net.add_link("na-west", "asia", latency=0.075, jitter=0.008)
    net.add_link("na-west", "sa-east", latency=0.060, jitter=0.006)
    net.add_link("na-west", "africa", latency=0.090, jitter=0.010)
    net.add_link("europe", "asia", latency=0.060, jitter=0.006)
    net.add_link("europe", "sa-east", latency=0.070, jitter=0.007)
    net.add_link("europe", "africa", latency=0.030, jitter=0.003)  # Close!
    net.add_link("asia", "sa-east", latency=0.120, jitter=0.012)
    net.add_link("asia", "africa", latency=0.080, jitter=0.008)
    net.add_link("sa-east", "africa", latency=0.085, jitter=0.009)
    return net


def interplanetary_topology(env: simpy.Environment, seed: int = 42) -> DatacenterNetwork:
    """Full topology: 5 DCs + LEO orbit + Moon + Mars."""
    net = five_dc_topology(env, seed)

    # LEO satellite (variable, ~20-40ms to ground)
    net.add_location("leo-sat")
    net.add_link("na-west", "leo-sat", latency=0.020, jitter=0.010)
    net.add_link("europe", "leo-sat", latency=0.025, jitter=0.010)
    net.add_link("asia", "leo-sat", latency=0.030, jitter=0.010)

    # Moon (~1.3s RTT = 0.65s one-way)
    net.add_location("moon")
    net.add_link("na-west", "moon", latency=0.650, jitter=0.020)
    net.add_link("europe", "moon", latency=0.650, jitter=0.020)

    # Mars (4-24 min one-way depending on orbital position)
    # Start at closest approach: ~4 min one-way
    net.add_location("mars")
    net.add_link("na-west", "mars", latency=240.0, jitter=5.0)  # 4 minutes
    net.add_link("europe", "mars", latency=240.0, jitter=5.0)

    return net
