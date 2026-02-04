"""Entity ID management for VMTP simulation.

Entities are 64-bit identifiers that are host-address independent.
For simulation purposes, we use simple sequential integers with
a registry to track entity-to-endpoint mappings.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Any


@dataclass
class Entity:
    """A VMTP entity - a transport-level endpoint."""

    id: int
    name: str = ""  # Human-readable name for debugging
    endpoint: Any = None  # The SimPy process/mailbox associated with this entity

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, Entity):
            return self.id == other.id
        return False

    def __repr__(self):
        if self.name:
            return f"Entity({self.id}, {self.name!r})"
        return f"Entity({self.id})"


class EntityRegistry:
    """Registry for entity ID allocation and lookup."""

    def __init__(self):
        self._next_id: int = 1
        self._entities: Dict[int, Entity] = {}
        self._by_name: Dict[str, Entity] = {}

    def create(self, name: str = "", endpoint: Any = None) -> Entity:
        """Allocate a new entity ID."""
        entity = Entity(
            id=self._next_id,
            name=name or f"entity-{self._next_id}",
            endpoint=endpoint,
        )
        self._next_id += 1
        self._entities[entity.id] = entity
        self._by_name[entity.name] = entity
        return entity

    def lookup(self, entity_id: int) -> Optional[Entity]:
        """Find entity by ID."""
        return self._entities.get(entity_id)

    def lookup_by_name(self, name: str) -> Optional[Entity]:
        """Find entity by name."""
        return self._by_name.get(name)

    def update_endpoint(self, entity: Entity, endpoint: Any) -> None:
        """Update the endpoint for an entity (e.g., after migration)."""
        entity.endpoint = endpoint
        self._entities[entity.id] = entity

    def __len__(self) -> int:
        return len(self._entities)
