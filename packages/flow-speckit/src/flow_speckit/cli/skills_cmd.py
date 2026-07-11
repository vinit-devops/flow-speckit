"""``flow-speckit skills list`` — list registered skills (doc 04 §3, doc 07 §1)."""

from __future__ import annotations

from pathlib import Path

import structlog
import typer
from rich.console import Console
from rich.table import Table

from flow_speckit.plugins import discover_entry_points, discover_local_skills
from flow_speckit.skills.registry import SkillRegistry

logger = structlog.get_logger(__name__)

skills_app = typer.Typer(
    name="skills",
    help="List registered skills with name, version, I/O types, tier, and provenance.",
)
console = Console()


def build_skill_registry(root: Path) -> SkillRegistry:
    """Build a fully-populated registry from entry points and local skills.

    Shared facade used by the CLI, SkillHarness, and the workflow engine so
    the discover-and-register loop lives in one place. Collisions between
    installed packages skip the colliding skill with a warning; discovery
    itself keeps going.
    """
    registry = SkillRegistry()
    for name, fn in discover_entry_points("flow_speckit.skills"):
        try:
            registry.register(fn, provenance=f"package:{name}")
        except RuntimeError as exc:
            logger.warning(
                "skill_registration_failed",
                provenance=f"package:{name}",
                error=str(exc),
            )
    for fn in discover_local_skills(root):
        try:
            registry.register(fn, provenance=f"local:{root / 'skills'}")
        except RuntimeError as exc:
            logger.warning(
                "skill_registration_failed",
                provenance=f"local:{root / 'skills'}",
                error=str(exc),
            )
    return registry


@skills_app.command("list")
def list_skills(
    root: Path | None = typer.Option(  # noqa: B008 — typer option factory
        None,
        "--root",
        "-r",
        help="Repository root (for local ./skills/ discovery); defaults to CWD.",
    ),
) -> None:
    """Show every registered skill with name, version, I/O types, tier, and provenance."""
    registry = build_skill_registry(root or Path.cwd())
    definitions = registry.list_all()
    if not definitions:
        console.print(
            "[dim]No skills registered. Add @skill functions or install a skill pack.[/dim]"
        )
        return

    table = Table(title="Registered Skills")
    table.add_column("NAME")
    table.add_column("VERSION")
    table.add_column("INPUT")
    table.add_column("OUTPUT")
    table.add_column("TIER")
    table.add_column("PROVENANCE")

    for d in definitions:
        inp = ", ".join(d.input_types) if d.input_types else "-"
        outp = d.output_type or "-"
        tier = d.llm.tier if d.llm else "-"
        table.add_row(d.name, d.version, inp, outp, tier, d.provenance)

    console.print(table)
