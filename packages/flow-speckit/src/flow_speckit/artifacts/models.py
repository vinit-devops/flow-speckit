from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

Status = Literal["draft", "proposed", "approved", "rejected", "superseded"]
Relation = Literal["derived_from", "supersedes", "informs", "implements", "reviews"]


class ArtifactModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    artifact_type: ClassVar[str]
    artifact_schema_version: ClassVar[int] = 1

    def __init_subclass__(
        cls, *, artifact_type: str | None = None, schema_version: int = 1, **kwargs: Any
    ) -> None:
        super().__init_subclass__(**kwargs)
        if artifact_type is not None:
            cls.artifact_type = artifact_type
            cls.artifact_schema_version = schema_version

    def render_md(self) -> str:
        lines = [f"# {self.artifact_type}"]
        for name, value in self.model_dump(mode="json").items():
            lines.append(f"**{name}:** {value}")
        return "\n\n".join(lines)


class GenericArtifact(ArtifactModel, artifact_type="generic"):
    title: str
    body: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)

    def render_md(self) -> str:
        return f"# {self.title}\n\n{self.body}"
