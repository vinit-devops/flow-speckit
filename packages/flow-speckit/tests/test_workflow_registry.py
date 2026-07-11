import importlib
import sys
from importlib.metadata import EntryPoint
from pathlib import Path
from types import ModuleType

import pytest

from flow_speckit.workflows import (
    UnknownWorkflow,
    WorkflowCollisionError,
    WorkflowDefinition,
    WorkflowRegistry,
    workflow,
)

# The `registry` module is shadowed by the re-exported singleton on the
# package, so monkeypatching by dotted string would hit the instance instead.
registry_module = importlib.import_module("flow_speckit.workflows.registry")


@workflow(name="feature", version="1")
async def feature(ctx: object, idea: str) -> str:
    """Ship a feature."""
    return f"shipped {idea}"


def make_definition(name: str = "wf", version: str = "1") -> WorkflowDefinition:
    @workflow(name=name, version=version)
    async def fn(ctx: object) -> None:
        pass

    return fn


# --- @workflow DSL ---


def test_decorator_produces_pinned_definition() -> None:
    assert isinstance(feature, WorkflowDefinition)
    assert feature.name == "feature"
    assert feature.version == "1"
    assert feature.key == ("feature", "1")
    assert feature.source_package == "local"


def test_decorator_preserves_wrapped_function() -> None:
    assert feature.fn.__name__ == "feature"
    assert feature.fn.__doc__ == "Ship a feature."
    assert hasattr(feature.fn, "__wrapped__")


async def test_definition_is_callable() -> None:
    assert await feature(None, "widgets") == "shipped widgets"
    assert await feature.fn(None, "widgets") == "shipped widgets"


def test_definition_is_frozen() -> None:
    with pytest.raises(Exception):  # noqa: B017 (dataclasses.FrozenInstanceError)
        feature.name = "renamed"  # type: ignore[misc]


def test_decorator_validates_name_and_version() -> None:
    async def fn(ctx: object) -> None:
        pass

    with pytest.raises(ValueError):
        workflow(name="", version="1")(fn)
    with pytest.raises(ValueError):
        workflow(name="wf", version="")(fn)


def test_decorator_rejects_sync_functions() -> None:
    def sync_fn(ctx: object) -> None:
        pass

    with pytest.raises(TypeError):
        workflow(name="wf", version="1")(sync_fn)  # type: ignore[arg-type]


# --- WorkflowRegistry ---


def test_register_get() -> None:
    reg = WorkflowRegistry()
    defn = make_definition("wf", "1")
    reg.register(defn, source_package="pkg-a")
    assert reg.get("wf", "1").fn is defn.fn
    assert reg.get("wf", "1").source_package == "pkg-a"
    with pytest.raises(UnknownWorkflow):
        reg.get("nope", "1")
    with pytest.raises(UnknownWorkflow):
        reg.get("wf", "2")


def test_versions_are_distinct_keys() -> None:
    reg = WorkflowRegistry()
    v1 = make_definition("wf", "1")
    v2 = make_definition("wf", "2")
    reg.register(v1, source_package="pkg-a")
    reg.register(v2, source_package="pkg-a")
    assert reg.get("wf", "1").fn is v1.fn
    assert reg.get("wf", "2").fn is v2.fn


def test_latest_prefers_highest_numeric_version() -> None:
    reg = WorkflowRegistry()
    for version in ("1", "2", "10"):
        reg.register(make_definition("wf", version))
    assert reg.latest("wf").version == "10"
    with pytest.raises(UnknownWorkflow):
        reg.latest("nope")


def test_reregistering_identical_definition_is_noop() -> None:
    reg = WorkflowRegistry()
    defn = make_definition("wf", "1")
    reg.register(defn, source_package="pkg-a")
    reg.register(defn, source_package="pkg-b")  # same fn: no-op, no collision
    assert reg.get("wf", "1").source_package == "pkg-a"


def test_installed_collision_raises() -> None:
    reg = WorkflowRegistry()
    reg.register(make_definition("wf", "1"), source_package="pkg-a")
    with pytest.raises(WorkflowCollisionError):
        reg.register(make_definition("wf", "1"), source_package="pkg-b")
    # A different definition from the SAME package also collides.
    with pytest.raises(WorkflowCollisionError):
        reg.register(make_definition("wf", "1"), source_package="pkg-a")


def test_local_overrides_installed() -> None:
    reg = WorkflowRegistry()
    reg.register(make_definition("wf", "1"), source_package="pkg-a")
    local = make_definition("wf", "1")
    reg.register(local, source_package="local")
    assert reg.get("wf", "1").fn is local.fn
    assert reg.get("wf", "1").source_package == "local"


def test_installed_after_local_is_ignored() -> None:
    reg = WorkflowRegistry()
    local = make_definition("wf", "1")
    reg.register(local, source_package="local")
    reg.register(make_definition("wf", "1"), source_package="pkg-a")
    assert reg.get("wf", "1").fn is local.fn
    assert reg.get("wf", "1").source_package == "local"


# --- entry points ---


def test_entry_point_load_failure_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    bad = EntryPoint("bad-wf", "nonexistent_module_xyz:defn", "flow_speckit.workflows")
    monkeypatch.setattr(registry_module, "entry_points", lambda group: [bad])
    reg = WorkflowRegistry()
    with pytest.raises(RuntimeError, match=r"'bad-wf'.*nonexistent_module_xyz:defn"):
        reg.load_entry_points()


def test_entry_point_loads_and_stamps_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = ModuleType("fake_wf_pkg")
    mod.defn = feature  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "fake_wf_pkg", mod)
    ep = EntryPoint("feature", "fake_wf_pkg:defn", "flow_speckit.workflows")
    monkeypatch.setattr(registry_module, "entry_points", lambda group: [ep])
    reg = WorkflowRegistry()
    reg.load_entry_points()
    loaded = reg.get("feature", "1")
    assert loaded.fn is feature.fn
    # No dist attached to a hand-built EntryPoint: falls back to the module.
    assert loaded.source_package == "fake_wf_pkg"


def test_entry_point_non_definition_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = ModuleType("fake_wf_pkg2")
    mod.not_a_workflow = object()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "fake_wf_pkg2", mod)
    ep = EntryPoint("oops", "fake_wf_pkg2:not_a_workflow", "flow_speckit.workflows")
    monkeypatch.setattr(registry_module, "entry_points", lambda group: [ep])
    reg = WorkflowRegistry()
    with pytest.raises(RuntimeError, match="did not resolve to a WorkflowDefinition"):
        reg.load_entry_points()


def test_load_entry_points_empty_group_is_noop() -> None:
    reg = WorkflowRegistry()
    reg.load_entry_points(group="flow_speckit.workflows.does-not-exist")
    assert reg.all() == []


# --- project-local discovery ---


def test_discover_local(tmp_path: Path) -> None:
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    (workflows_dir / "local_wf.py").write_text(
        "from flow_speckit.workflows import workflow\n"
        "\n"
        '@workflow(name="local-wf", version="1")\n'
        "async def local_wf(ctx):\n"
        '    return "ok"\n'
    )
    reg = WorkflowRegistry()
    reg.discover_local(tmp_path)
    defn = reg.get("local-wf", "1")
    assert defn.source_package == "local"
    assert defn.fn.__name__ == "local_wf"


def test_discover_local_overrides_installed(tmp_path: Path) -> None:
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    (workflows_dir / "override_wf.py").write_text(
        "from flow_speckit.workflows import workflow\n"
        "\n"
        '@workflow(name="feature", version="1")\n'
        "async def local_feature(ctx):\n"
        '    return "local wins"\n'
    )
    reg = WorkflowRegistry()
    reg.register(make_definition("feature", "1"), source_package="pkg-a")
    reg.discover_local(tmp_path)
    assert reg.get("feature", "1").source_package == "local"
    assert reg.get("feature", "1").fn.__name__ == "local_feature"


def test_discover_local_missing_dir_is_noop(tmp_path: Path) -> None:
    reg = WorkflowRegistry()
    reg.discover_local(tmp_path)  # no ./workflows/ directory
    assert reg.all() == []


def test_discover_local_import_failure_is_wrapped(tmp_path: Path) -> None:
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    (workflows_dir / "broken_wf.py").write_text('raise ValueError("boom")\n')
    reg = WorkflowRegistry()
    with pytest.raises(RuntimeError, match="broken_wf"):
        reg.discover_local(tmp_path)
