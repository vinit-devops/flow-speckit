"""``flow-speckit skills list`` — list registered skills (doc 04 §3, doc 07 §1)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from flow_speckit.plugins import discover_entry_points, discover_local_skills
from flow_speckit.skills.base import SkillDefinition
from flow_speckit.skills.registry import SkillRegistry

skills_app = typer.Typer(
    name="skills",
    help="List registered skills with name, version, I/O types, tier, and provenance.",
)
console = Console()


@skills_app.command("list")
def list_skills(
    root: Path = typer.Option(
        Path.cwd(),
        "--root",
        "-r",
        help="Repository root (for local ./skills/ discovery).",
    ),
) -> None:
    """Show every registered skill with name, version, I/O types, tier, and
    provenance (package vs local path)."""
    registry = SkillRegistry()

    # 1. Entry-point skills (installed packages)
    for name, fn in discover_entry_points("flow_speckit.skills"):
        try:
            registry.register(fn, provenance=f"package:{name}")
        except RuntimeError as exc:
            console.print(f"[yellow]Warning:[/yellow] {exc}")

    # 2. Project-local skills (./skills/*.py)
    for fn in discover_local_skills(root):
        try:
            registry.register(fn, provenance=f"local:{root / 'skills'}")
        except RuntimeError as exc:
            console.print(f"[yellow]Warning:[/yellow] {exc}")

    definitions = registry.list_all()
    if not definitions:
        console.print("[dim]No skills registered. Add @skill functions or install a skill pack.[/dim]")
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