"""LLMClient — LiteLLM wrapper with tier routing and cost accounting (doc 06 §§2, 4).

All LLM calls flow through this single seam; if LiteLLM's dependency weight
ever becomes a problem, this is the only file that knows it exists.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from flow_speckit.llm.tiers import LLMSpec, Tier, resolve_tier

if TYPE_CHECKING:
    from flow_speckit.llm.assemble import AssembledContext


class BudgetExceededError(Exception):
    """Raised when a skill's cost exceeds its ``max_cost_usd`` or the run budget."""


class SkillOutputError(Exception):
    """Response validation failed; retryable per step policy."""


class _NullLLMClient:
    """No-op client used when liteLLM is not installed — raises actionable
    errors so the user knows exactly what to install."""

    async def complete(
        self,
        prompt: str,
        *,
        context: AssembledContext | None = None,
        response_model: type[BaseModel] | None = None,
        tier: str | None = None,
        skill_name: str | None = None,
    ) -> BaseModel | str:
        raise RuntimeError(
            "liteLLM is not installed. Install it with: "
            "pip install 'flow-speckit[llm]'"
        )

    async def complete_streaming(self, prompt: str, **kwargs: Any) -> Any:
        raise RuntimeError(
            "liteLLM is not installed. Install it with: "
            "pip install 'flow-speckit[llm]'"
        )


class LLMClient:
    """Tier-routed LiteLLM client with cost metering (doc 06 §2).

    Resolution per call: spec tier → per-skill override → tier map → model
    name. Structured output via ``response_model`` with one repair round-trip
    on validation failure, then ``SkillOutputError``.

    Budgets are enforced pre-flight: a call is refused with
    ``BudgetExceededError`` once the run has spent ``default_max_usd_per_run``
    or the skill has spent its ``max_cost_usd`` — an in-flight completion is
    never discarded after the money is already spent.
    """

    def __init__(
        self,
        tier_map: dict[str, str],
        *,
        overrides: dict[str, str] | None = None,
        default_max_usd_per_run: float = 25.0,
    ) -> None:
        self._tier_map = dict(tier_map)
        self._overrides = dict(overrides or {})
        self._default_max_usd_per_run = default_max_usd_per_run
        self._total_cost_usd: float = 0.0
        self._cost_by_skill: dict[str, float] = {}
        self._litellm: Any = None

    @property
    def total_cost_usd(self) -> float:
        return self._total_cost_usd

    def cost_for_skill(self, skill_name: str) -> float:
        return self._cost_by_skill.get(skill_name, 0.0)

    def _ensure_litellm(self) -> Any:
        """Return the litellm module, or a ``_NullLLMClient`` when not installed."""
        if self._litellm is not None:
            return self._litellm
        try:
            import litellm  # type: ignore[import-untyped]
        except ImportError:
            self._litellm = _NullLLMClient()
        else:
            self._litellm = litellm
        return self._litellm

    def _resolve_model(self, tier: Tier, skill_name: str | None = None) -> str:
        return resolve_tier(
            LLMSpec(tier=tier),
            self._tier_map,
            skill_name=skill_name,
            overrides=self._overrides,
        )

    def _build_messages(
        self, prompt: str, context: AssembledContext | None
    ) -> list[dict[str, Any]]:
        if context is not None:
            return [
                {"role": "system", "content": context.render()},
                {"role": "user", "content": prompt},
            ]
        return [{"role": "user", "content": prompt}]

    async def complete(
        self,
        prompt: str,
        *,
        context: AssembledContext | None = None,
        response_model: type[BaseModel] | None = None,
        tier: Tier = "standard",
        skill_name: str | None = None,
        max_cost_usd: float = 5.0,
    ) -> BaseModel | str:
        """Run a completion through the tier-resolved model.

        If ``response_model`` is given, structured output via provider-native
        structured/tool-call modes is requested; the result is validated by
        Pydantic with one repair round-trip on failure.
        """
        litellm = self._ensure_litellm()
        if isinstance(litellm, _NullLLMClient):
            return await litellm.complete(prompt, tier=tier)

        model = self._resolve_model(tier, skill_name)
        messages = self._build_messages(prompt, context)

        kwargs: dict[str, Any] = {"model": model, "messages": messages}
        if response_model is not None:
            kwargs["response_format"] = response_model

        for round_num in (1, 2):
            self._check_budget(skill_name, max_cost_usd)
            try:
                raw = await litellm.acompletion(**kwargs)
            except Exception as exc:
                raise SkillOutputError(f"LLM call failed: {exc}") from exc

            self._record_cost(raw, skill_name)

            message = raw.choices[0].message
            content = message.content

            if response_model is None:
                if isinstance(content, str):
                    return content
                return str(content)

            try:
                parsed = getattr(message, "parsed", None)
                if parsed:
                    return response_model.model_validate(parsed)
                return response_model.model_validate_json(content or "")
            except Exception as exc:
                if round_num == 2:
                    raise SkillOutputError(
                        f"Structured output validation failed: {exc}"
                    ) from exc
                # Repair round: show the model its own output plus the error.
                messages.append(
                    {"role": "assistant", "content": str(content or "")}
                )
                messages.append({
                    "role": "user",
                    "content": f"Previous output did not match the expected schema. "
                    f"Error: {exc}. Please retry with valid JSON.",
                })
                kwargs["messages"] = messages

        # unreachable, but type-checker doesn't know
        raise SkillOutputError("Unexpected state in complete()")

    async def complete_streaming(
        self,
        prompt: str,
        *,
        context: AssembledContext | None = None,
        tier: Tier = "standard",
        skill_name: str | None = None,
    ) -> Any:
        """Streaming variant (doc 06 §2); returns an async iterator of chunks.

        Streamed responses are not cost-metered chunk-by-chunk; the run
        budget is still checked before the call goes out.
        """
        litellm = self._ensure_litellm()
        if isinstance(litellm, _NullLLMClient):
            return await litellm.complete_streaming(prompt)

        self._check_budget(skill_name, float("inf"))
        model = self._resolve_model(tier, skill_name)
        messages = self._build_messages(prompt, context)

        return await litellm.acompletion(model=model, messages=messages, stream=True)

    def _check_budget(self, skill_name: str | None, max_cost_usd: float) -> None:
        """Refuse the next call once a budget is exhausted (doc 06 §4)."""
        if self._total_cost_usd >= self._default_max_usd_per_run:
            raise BudgetExceededError(
                f"Run LLM budget exhausted: spent "
                f"${self._total_cost_usd:.2f} of the "
                f"${self._default_max_usd_per_run:.2f} per-run budget"
            )
        if skill_name is not None:
            spent = self._cost_by_skill.get(skill_name, 0.0)
            if spent >= max_cost_usd:
                raise BudgetExceededError(
                    f"Skill {skill_name!r} LLM budget exhausted: spent "
                    f"${spent:.2f} of its ${max_cost_usd:.2f} budget"
                )

    def _record_cost(self, raw: Any, skill_name: str | None) -> None:
        """Accumulate the call's USD cost from LiteLLM's response metadata."""
        cost = 0.0
        hidden = getattr(raw, "_hidden_params", None)
        if isinstance(hidden, dict):
            cost = float(hidden.get("response_cost") or 0.0)
        if not cost and self._litellm is not None and not isinstance(
            self._litellm, _NullLLMClient
        ):
            try:
                cost = float(
                    self._litellm.completion_cost(completion_response=raw) or 0.0
                )
            except Exception:
                cost = 0.0
        self._total_cost_usd += cost
        if skill_name is not None:
            self._cost_by_skill[skill_name] = (
                self._cost_by_skill.get(skill_name, 0.0) + cost
            )
