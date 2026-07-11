"""Phase 4 unit tests: Skill Engine, LLM & Context, Execution Engine, Git, Config.

Tests the four new kernel subsystems plus config and plugin infrastructure.
Does NOT require a database — uses in-memory fakes and mock objects.
"""

from __future__ import annotations

import json
from datetime import UTC

import pytest

from flow_speckit.artifacts.models import ArtifactModel, GenericArtifact
from flow_speckit.config import FlowSpeckitSettings
from flow_speckit.execution.base import (
    BackendHealth,
    ExecutionConstraints,
    ExecutionResult,
    ExecutionTask,
    Workspace,
)
from flow_speckit.execution.local_shell import LocalShellBackend
from flow_speckit.git.local import LocalGitProvider
from flow_speckit.git.provider import RepoRef
from flow_speckit.llm.assemble import AssembledContext, ContextAssembler, ContextChunk
from flow_speckit.llm.tiers import LLMSpec, resolve_tier
from flow_speckit.skills.base import SkillContext, skill
from flow_speckit.skills.registry import SkillRegistry, UnknownSkill
from flow_speckit.skills.testing import RecordedLLM, SkillHarness


class ExampleOutput(ArtifactModel, artifact_type="example_output"):
    value: str = ""


# ==========================================================================
# 1. LLM Tiers (doc 06 §2)
# ==========================================================================


class TestLLMSpec:
    def test_default_tier(self):
        spec = LLMSpec()
        assert spec.tier == "standard"
        assert spec.max_cost_usd == 5.0

    def test_reasoning_tier(self):
        spec = LLMSpec(tier="reasoning", max_cost_usd=10.0)
        assert spec.tier == "reasoning"
        assert spec.max_cost_usd == 10.0

    def test_immutable(self):
        from pydantic import ValidationError

        spec = LLMSpec(tier="fast")
        with pytest.raises(ValidationError):
            spec.tier = "standard"  # type: ignore[misc]


class TestResolveTier:
    def test_direct_resolution(self):
        spec = LLMSpec(tier="fast")
        model = resolve_tier(spec, {"fast": "anthropic/claude-haiku-4-5"})
        assert model == "anthropic/claude-haiku-4-5"

    def test_skill_override_wins(self):
        spec = LLMSpec(tier="standard")
        model = resolve_tier(
            spec,
            {"standard": "anthropic/claude-sonnet-5"},
            skill_name="code_review",
            overrides={"code_review": "openai/gpt-5"},
        )
        assert model == "openai/gpt-5"

    def test_missing_tier_raises(self):
        spec = LLMSpec(tier="reasoning")
        with pytest.raises(KeyError, match="No model configured for tier"):
            resolve_tier(spec, {})


# ==========================================================================
# 2. Context Assembler (doc 06 §5)
# ==========================================================================


class TestContextAssembler:
    async def test_assemble_primary_only(self):
        assembler = ContextAssembler(None)  # type: ignore[arg-type]
        artifact = GenericArtifact(title="Test", body="Hello world")
        result = await assembler.assemble(artifact, include="primary-only")
        assert len(result.chunks) == 1
        assert result.chunks[0].fidelity == "full"
        assert "Test" in result.chunks[0].content
        assert result.total_tokens > 0

    def test_render(self):
        ctx = AssembledContext(
            primary_refs=["test/foo@1"],
            chunks=[
                ContextChunk(
                    ref_address="test/foo@1", fidelity="full", content="# Test\nbody"
                )
            ],
            total_tokens=10,
        )
        rendered = ctx.render()
        assert "test/foo@1" in rendered
        assert "fidelity=full" in rendered

    def test_estimate_tokens(self):
        assembler = ContextAssembler(None)  # type: ignore[arg-type]
        chunks = [ContextChunk("a", "full", "12345678")]
        assert assembler._estimate_tokens(chunks) == 2


# ==========================================================================
# 3. Skill Engine — @skill decorator (doc 04 §1)
# ==========================================================================


class TestSkillDecorator:
    def test_records_metadata(self):
        @skill(name="test_skill", input=GenericArtifact, output=ExampleOutput, version="2.0")
        async def test_fn(input_model, ctx):
            return ExampleOutput(value="done")

        assert test_fn._flow_speckit_skill is True
        assert test_fn._skill_definition.name == "test_skill"
        assert test_fn._skill_definition.version == "2.0"
        assert test_fn._skill_definition.input_types == ["generic"]
        assert test_fn._skill_definition.output_type == "example_output"

    def test_default_version(self):
        @skill(name="default_version")
        async def fn1(ctx):
            return "ok"

        assert fn1._skill_definition.version == "0.1.0"

    def test_multi_input(self):
        @skill(name="multi", input=(GenericArtifact, ExampleOutput))
        async def multi_fn(a, b, ctx):
            return ExampleOutput(value="merged")

        assert multi_fn._skill_definition.input_types == ["generic", "example_output"]


# ==========================================================================
# 4. Skill Engine — SkillContext
# ==========================================================================


class TestSkillContext:
    def test_basic_context(self):
        ctx = SkillContext(skill_name="test", run_id="r1", step_key="s1")
        assert ctx.skill_name == "test"
        assert ctx.run_id == "r1"
        assert ctx.step_key == "s1"

    def test_config_is_read_only(self):
        ctx = SkillContext(skill_name="test", config={"key": "value"})
        assert ctx.config["key"] == "value"
        with pytest.raises(TypeError):
            ctx.config["key"] = "new"  # type: ignore[index]


# ==========================================================================
# 5. Skill Registry (doc 04 §3)
# ==========================================================================


class TestSkillRegistry:
    def test_register_and_get(self):
        reg = SkillRegistry()

        @skill(name="my_skill", version="1.0")
        async def fn1(ctx):
            pass

        reg.register(fn1)
        d = reg.get("my_skill")
        assert d.name == "my_skill"
        assert d.version == "1.0"

    def test_unknown_skill_raises(self):
        reg = SkillRegistry()
        with pytest.raises(UnknownSkill):
            reg.get("nonexistent")

    def test_local_overrides_installed(self):
        reg = SkillRegistry()

        @skill(name="shared", version="1.0")
        async def local_fn(ctx):
            pass

        @skill(name="shared", version="1.0")
        async def installed_fn(ctx):
            pass

        reg.register(installed_fn, provenance="package:skills")
        reg.register(local_fn, provenance="local:./skills")
        d = reg.get("shared")
        assert d.provenance.startswith("local")

    def test_two_installed_collision_is_error(self):
        reg = SkillRegistry()

        @skill(name="dup", version="1.0")
        async def a_fn(ctx):
            pass

        @skill(name="dup", version="1.0")
        async def b_fn(ctx):
            pass

        reg.register(a_fn, provenance="package:skills-a")
        with pytest.raises(RuntimeError, match="already registered from"):
            reg.register(b_fn, provenance="package:skills-b")

    def test_list_all_sorted(self):
        reg = SkillRegistry()

        @skill(name="z_skill", version="1.0")
        async def z_fn(ctx):
            pass

        @skill(name="a_skill", version="2.0")
        async def a_fn(ctx):
            pass

        reg.register(a_fn)
        reg.register(z_fn)
        all_skills = reg.list_all()
        assert all_skills[0].name == "a_skill"
        assert all_skills[1].name == "z_skill"

    def test_get_latest_version(self):
        reg = SkillRegistry()

        @skill(name="multi", version="1.0")
        async def v1(ctx):
            pass

        @skill(name="multi", version="2.0")
        async def v2(ctx):
            pass

        reg.register(v1)
        reg.register(v2)
        d = reg.get("multi")
        assert d.version == "2.0"

    def test_get_pinned_version(self):
        reg = SkillRegistry()

        @skill(name="pinned", version="1.0")
        async def v1(ctx):
            pass

        @skill(name="pinned", version="2.0")
        async def v2(ctx):
            pass

        reg.register(v1)
        reg.register(v2)
        d = reg.get("pinned", version="1.0")
        assert d.version == "1.0"


# ==========================================================================
# 6. Skill Testing Harness (doc 04 §4)
# ==========================================================================


class TestRecordedLLM:
    async def test_replay(self, tmp_path):
        fixture = tmp_path / "recordings.json"
        fixture.write_text(json.dumps({"my_skill": {"value": "replayed"}}))
        llm = RecordedLLM(fixture)
        result = await llm.complete(
            "ignored", response_model=ExampleOutput, skill_name="my_skill"
        )
        assert result.value == "replayed"

    async def test_missing_key_returns_empty(self, tmp_path):
        fixture = tmp_path / "empty.json"
        fixture.write_text(json.dumps({}))
        llm = RecordedLLM(fixture)
        result = await llm.complete("anything")
        assert result == ""


class TestSkillHarness:
    async def test_run_skill(self):
        reg = SkillRegistry()

        @skill(name="echo", input=GenericArtifact, output=ExampleOutput)
        async def echo_fn(inp, ctx):
            return ExampleOutput(value=inp.title)

        reg.register(echo_fn)
        harness = SkillHarness(registry=reg)
        inp = GenericArtifact(title="hello")
        result = await harness.run("echo", inp)
        assert result.value == "hello"


# ==========================================================================
# 7. Execution Engine (doc 05)
# ==========================================================================


class TestExecutionModels:
    def test_task_defaults(self):
        task = ExecutionTask(instructions="do something")
        assert task.constraints.timeout_s == 600

    def test_backend_health(self):
        health = BackendHealth(available=True, version="1.0")
        assert health.available is True

    def test_execution_result(self):
        result = ExecutionResult(status="completed", summary="done")
        assert result.cost.usd == 0.0
        assert result.cost.estimated is True


class TestLocalShellBackend:
    async def test_check_available(self):
        backend = LocalShellBackend()
        health = await backend.check_available()
        assert health.available is True
        assert health.version is not None

    async def test_execute_trivial(self, tmp_path):
        backend = LocalShellBackend()
        ws = Workspace(
            path=tmp_path, repo=".", base_branch="main", target_branch="test"
        )
        task = ExecutionTask(
            instructions='echo "hello world"',
            constraints=ExecutionConstraints(timeout_s=30),
        )
        events = []

        async def sink(msg):
            events.append(msg)

        result = await backend.execute(task, ws, sink)
        assert result.status == "completed"

    async def test_execute_timeout(self, tmp_path):
        backend = LocalShellBackend()
        ws = Workspace(
            path=tmp_path, repo=".", base_branch="main", target_branch="test"
        )
        task = ExecutionTask(
            instructions="sleep 60",
            constraints=ExecutionConstraints(timeout_s=1),
        )
        events = []

        async def sink(msg):
            events.append(msg)

        result = await backend.execute(task, ws, sink)
        assert result.status == "failed"


# ==========================================================================
# 8. Git Provider (doc 05 §6)
# ==========================================================================


class TestGitProvider:
    async def test_pr_not_implemented_locally(self):
        provider = LocalGitProvider()
        with pytest.raises(NotImplementedError):
            await provider.open_pr(RepoRef(), "head", "base", "title", "body")


# ==========================================================================
# 9. Config (doc 06 §2)
# ==========================================================================


class TestConfig:
    def test_default_values(self):
        settings = FlowSpeckitSettings()
        assert settings.database_url is None
        assert settings.llm.tiers == {}
        assert settings.llm.default_max_usd_per_run == 25.0
        assert settings.execution.backend == "local_shell"

    def test_load_from_toml(self, tmp_path):
        toml_path = tmp_path / "flow-speckit.toml"
        toml_path.write_text(
            """\
[database]
url = "postgresql://localhost/test"
[llm.tiers]
fast = "anthropic/claude-haiku-4-5"
standard = "anthropic/claude-sonnet-5"
[llm.budget]
default_max_usd_per_run = 50.0
[execution]
backend = "local_shell"
"""
        )
        settings = FlowSpeckitSettings.load(root=tmp_path)
        assert settings.database_url == "postgresql://localhost/test"
        assert settings.llm.tiers["fast"] == "anthropic/claude-haiku-4-5"
        assert settings.llm.default_max_usd_per_run == 50.0
        assert settings.execution.backend == "local_shell"


# ==========================================================================
# 10. Plugin system
# ==========================================================================


class TestPlugins:
    def test_discover_local_skills_empty(self, tmp_path):
        from flow_speckit.plugins import discover_local_skills

        skills = list(discover_local_skills(tmp_path))
        assert skills == []


# ==========================================================================
# 11. LLM cost accounting and budget enforcement (doc 06 §4)
# ==========================================================================


class _FakeRaw:
    """Mimics a LiteLLM ModelResponse's cost metadata."""

    def __init__(self, cost: float) -> None:
        self._hidden_params = {"response_cost": cost}


class TestLLMBudget:
    def test_record_cost_reads_hidden_params(self):
        from flow_speckit.llm.client import LLMClient

        client = LLMClient({"standard": "m"})
        client._record_cost(_FakeRaw(1.25), "my_skill")
        assert client.total_cost_usd == 1.25
        assert client.cost_for_skill("my_skill") == 1.25

    def test_run_budget_enforced(self):
        from flow_speckit.llm.client import BudgetExceededError, LLMClient

        client = LLMClient({"standard": "m"}, default_max_usd_per_run=2.0)
        client._record_cost(_FakeRaw(2.5), "s")
        with pytest.raises(BudgetExceededError, match="Run LLM budget"):
            client._check_budget("s", 100.0)

    def test_skill_budget_enforced(self):
        from flow_speckit.llm.client import BudgetExceededError, LLMClient

        client = LLMClient({"standard": "m"}, default_max_usd_per_run=100.0)
        client._record_cost(_FakeRaw(6.0), "spender")
        with pytest.raises(BudgetExceededError, match="'spender'"):
            client._check_budget("spender", 5.0)
        # Other skills are unaffected by one skill's exhaustion
        client._check_budget("other", 5.0)


# ==========================================================================
# 12. Backend registry
# ==========================================================================


class TestBackendRegistry:
    def test_get_local_shell(self):
        from flow_speckit.execution.backend_registry import BackendRegistry

        registry = BackendRegistry()
        backend = registry.get("local_shell")
        assert backend.name == "local_shell"

    def test_unknown_backend_raises(self):
        from flow_speckit.execution.backend_registry import BackendRegistry

        registry = BackendRegistry()
        with pytest.raises(KeyError, match="No backend named"):
            registry.get("nonexistent")


# ==========================================================================
# 13. Execution conformance suite against local_shell
# ==========================================================================


class TestConformanceSuite:
    async def test_local_shell_passes(self, tmp_path):
        from flow_speckit.execution.testing import run_conformance_suite

        backend = LocalShellBackend()
        ws = Workspace(
            path=tmp_path, repo=".", base_branch="main", target_branch="t"
        )
        await run_conformance_suite(backend, ws)


# ==========================================================================
# 14. WorkspaceManager worktree lifecycle (real git)
# ==========================================================================


class TestWorkspaceManager:
    async def test_worktree_lifecycle(self, tmp_path):
        import subprocess

        from flow_speckit.execution.workspace import WorkspaceManager

        repo = tmp_path / "repo"
        repo.mkdir()

        def git(*args: str) -> str:
            return subprocess.run(
                ["git", "-C", str(repo), *args],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()

        git("init", "-b", "main")
        git("config", "user.email", "test@test")
        git("config", "user.name", "test")
        (repo / "a.txt").write_text("one\n")
        git("add", "-A")
        git("commit", "-m", "init")

        manager = WorkspaceManager()
        ws = await manager.prepare(repo, "main", "run1")
        assert ws.path.exists()
        # prepare must not switch the main working tree off its branch
        assert git("rev-parse", "--abbrev-ref", "HEAD") == "main"

        (ws.path / "b.txt").write_text("two\n")
        commits, diff = await manager.capture(ws)
        assert len(commits) == 1
        assert "b.txt" in diff

        await manager.cleanup(ws, keep_on_failure=False)
        assert not ws.path.exists()
        assert git("branch", "--list", "flow-speckit/run-run1") == ""


# ==========================================================================
# 15. ContextAssembler lineage walking (doc 06 §5)
# ==========================================================================


class _FakeLineageStore:
    """Stands in for ArtifactStore: one primary with one stored ancestor."""

    def __init__(self, graph, bodies):
        self._graph = graph
        self._bodies = bodies

    async def lineage(self, ref, *, direction="up", max_depth=32):
        return self._graph

    async def get_body_md(self, ref):
        return self._bodies.get(ref)


class TestContextAssemblerLineage:
    def _make_store(self):
        from datetime import datetime
        from uuid import uuid4

        from flow_speckit.artifacts.graph import LineageEdge, LineageGraph
        from flow_speckit.artifacts.refs import ArtifactRef

        root_id, parent_id = uuid4(), uuid4()

        def ref(rid, key):
            return ArtifactRef(
                id=rid,
                type="generic",
                key=key,
                version=1,
                status="approved",
                content_hash="x",
                created_at=datetime.now(UTC),
            )

        graph = LineageGraph(
            root=root_id,
            nodes=[ref(root_id, "child"), ref(parent_id, "parent")],
            edges=[
                LineageEdge(
                    from_id=root_id, to_id=parent_id, relation="derived_from"
                )
            ],
        )
        return _FakeLineageStore(
            graph, {parent_id: "# Parent body\n" + "x" * 4000}
        )

    async def test_lineage_ancestors_included(self):
        store = self._make_store()
        assembler = ContextAssembler(store)  # type: ignore[arg-type]
        artifact = GenericArtifact(title="Child", body="child body")
        result = await assembler.assemble(
            artifact, primary_refs=["child@1"], include="lineage"
        )
        addresses = [c.ref_address for c in result.chunks]
        assert addresses[0] == "child@1"
        assert "parent@1" in addresses
        parent_chunk = next(c for c in result.chunks if c.ref_address == "parent@1")
        assert parent_chunk.fidelity == "full"
        assert "Parent body" in parent_chunk.content

    async def test_tiny_budget_degrades_to_title(self):
        store = self._make_store()
        assembler = ContextAssembler(store)  # type: ignore[arg-type]
        artifact = GenericArtifact(title="Child", body="child body")
        result = await assembler.assemble(
            artifact, primary_refs=["child@1"], budget_tokens=8
        )
        parent_chunk = next(c for c in result.chunks if c.ref_address == "parent@1")
        assert parent_chunk.fidelity == "title"

    async def test_no_refs_falls_back_to_primary_only(self):
        store = self._make_store()
        assembler = ContextAssembler(store)  # type: ignore[arg-type]
        artifact = GenericArtifact(title="Child", body="child body")
        result = await assembler.assemble(artifact)
        assert len(result.chunks) == 1
