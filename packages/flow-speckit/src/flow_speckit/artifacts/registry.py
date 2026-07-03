from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import entry_points

import structlog
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.artifacts.models import ArtifactModel
from flow_speckit.storage.schema import artifact_types

logger = structlog.get_logger(__name__)

_LOCAL = "local"


class UnknownArtifactType(KeyError):
    """Raised when looking up an artifact type that has not been registered."""


class RegistryCollisionError(RuntimeError):
    """Raised when two installed packages register the same artifact type name."""


@dataclass(frozen=True)
class RegisteredType:
    cls: type[ArtifactModel]
    source_package: str


class ArtifactRegistry:
    def __init__(self) -> None:
        self._types: dict[str, RegisteredType] = {}

    def register(self, cls: type[ArtifactModel], source_package: str = "local") -> None:
        name = cls.artifact_type
        existing = self._types.get(name)
        if existing is None:
            self._types[name] = RegisteredType(cls=cls, source_package=source_package)
            return

        if existing.cls is cls:
            return  # re-registering the identical class is a no-op

        if source_package == _LOCAL:
            # A local registration always overrides whatever was there before.
            logger.warning(
                "artifact_type_local_override",
                artifact_type=name,
                previous_source=existing.source_package,
                new_source=source_package,
            )
            self._types[name] = RegisteredType(cls=cls, source_package=source_package)
            return

        if existing.source_package == _LOCAL:
            # An installed package must never silently clobber a local override.
            logger.warning(
                "artifact_type_local_override_kept",
                artifact_type=name,
                local_source=existing.source_package,
                ignored_source=source_package,
            )
            return

        raise RegistryCollisionError(
            f"Artifact type {name!r} is already registered by package "
            f"{existing.source_package!r}; cannot re-register from package "
            f"{source_package!r}"
        )

    def get(self, name: str, schema_version: int | None = None) -> type[ArtifactModel]:
        registered = self._types.get(name)
        if registered is None:
            raise UnknownArtifactType(name)
        if schema_version is not None and registered.cls.artifact_schema_version != schema_version:
            raise UnknownArtifactType(f"{name} (schema_version={schema_version})")
        return registered.cls

    def load_entry_points(self) -> None:
        for ep in entry_points(group="flow_speckit.artifacts"):
            cls = ep.load()
            source_package = ep.dist.name if ep.dist is not None else ep.module
            self.register(cls, source_package=source_package)

    async def sync_to_db(self, session: AsyncSession) -> None:
        for registered in self._types.values():
            cls = registered.cls
            stmt = insert(artifact_types).values(
                name=cls.artifact_type,
                schema_version=cls.artifact_schema_version,
                json_schema=cls.model_json_schema(),
                source_package=registered.source_package,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["name", "schema_version"],
                set_={
                    "json_schema": stmt.excluded.json_schema,
                    "source_package": stmt.excluded.source_package,
                },
            )
            await session.execute(stmt)
        await session.commit()

    def all(self) -> list[RegisteredType]:
        return list(self._types.values())


registry = ArtifactRegistry()
