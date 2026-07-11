"""LLMClient — LiteLLM wrapper with tier routing and cost accounting (doc 06 §§2, 4).

All LLM calls flow through this single seam; if LiteLLM's dependency weight
ever becomes a problem, this is the only file that knows it exists.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from flow_speckit.llm.tiers import Tier

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
        self._client: Any = None

    @property
    def total_cost_usd(self) -> float:
        return self._total_cost_usd

    async def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from litellm import Router  # type: ignore[import-untyped]
        except ImportError:
            return _NullLLMClient()
        models = [
            {"model_name": model, "litellm_params": {"model": model}}
            for model in self._tier_map.values()
        ]
        self._client = Router(
            model_list=models,
            num_retries=1,
            fallbacks=[],
        )
        return self._client

    def _resolve_model(self, tier: Tier, skill_name: str | None = None) -> str:
        from flow_speckit.llm.tiers import resolve_tier

        from flow_speckit.llm.tiers import LLMSpec

        return resolve_tier(
            LLMSpec(tier=tier),
            self._tier_map,
            skill_name=skill_name,
            overrides=self._overrides,
        )

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
        client = await self._ensure_client()
        if isinstance(client, _NullLLMClient):
            return await client.complete(prompt, tier=tier)

        model = self._resolve_model(tier, skill_name)

        messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
        if context is not None:
            context_str = context.render()
            messages = [
                {"role": "system", "content": context_str},
                {"role": "user", "content": prompt},
            ]

        kwargs: dict[str, Any] = {"model": model, "messages": messages}
        if response_model is not None:
            kwargs["response_format"] = response_model

        for round_num in (1, 2):
            try:
                from litellm import completion  # type: ignore[import-untyped]

                raw = await completion(**kwargs)
            except Exception as exc:
                raise SkillOutputError(f"LLM call failed: {exc}") from exc

            self._accumulate_cost(raw)

            if response_model is None:
                content = raw.choices[0].message.content
                if isinstance(content, str):
                    return content
                return str(content)

            try:
                if hasattr(raw.choices[0].message, "parsed") and raw.choices[0].message.parsed:
                    return response_model.model_validate(
                        raw.choices[0].message.parsed
                    )
                content = raw.choices[0].message.content or ""
                return response_model.model_validate_json(content)
            except Exception as exc:
                if round_num == 2:
                    raise SkillOutputError(
                        f"Structured output validation failed: {exc}"
                    ) from exc
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
        """Streaming variant (doc 06 §2); returns an async iterator of chunks."""
        client = await self._ensure_client()
        if isinstance(client, _NullLLMClient):
            return await client.complete_streaming(prompt)

        model = self._resolve_model(tier, skill_name)

        messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
        if context is not None:
            context_str = context.render()
            messages = [
                {"role": "system", "content": context_str},
                {"role": "user", "content": prompt},
            ]

        from litellm import completion  # type: ignore[import-untyped]

        return await completion(model=model, messages=messages, stream=True)

    def _accumulate_cost(self, raw: Any) -> None:
        try:
            usage = raw.usage
            if usage is not None:
                cost = getattr(raw, "_response_cost", 0.0) or 0.0
                self._total_cost_usd += cost
        except Exception:
            pass