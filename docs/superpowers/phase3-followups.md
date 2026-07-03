# Phase 3 follow-ups (deferred by Phase 2 final review)

Items triaged as "defer" during the Phase 2 whole-branch review — pick these up
when the touching subsystem is next worked on.

- **Doc amendment:** rejected artifact versions are terminal — the `supersedes`
  edge skips them. Codified in `store.create()` + tests; amend
  `docs/design/02-artifact-engine.md` §5 to record the semantics.
- **registry.sync_to_db composability:** commits internally; extract transaction
  control when a second caller appears (workflow engine).
- **ArtifactStore.get(as_of_version=…):** in doc 02 §8, not yet implemented
  (Phase 2 plan omitted it deliberately).
- **Advisory lock namespacing:** `pg_advisory_xact_lock(hashtext(key))` uses the
  single-arg shared keyspace; move to the two-arg form with a flow-speckit class id
  before other subsystems take advisory locks.
- **Search:** add a rank tie-breaker (`, id`) and consider hoisting the repeated
  `plainto_tsquery` expression.
- **config.py:** wrap malformed `flow-speckit.toml` errors with a friendly message;
  revisit the env-vs-toml merge short-circuit before `FlowSpeckitSettings` grows
  fields.
- **CLI:** nested try/finally so `engine.dispose()` runs even if
  `session.close()` raises (4 sites in `artifacts_cmd.py`); consider `version`
  as the `list()` tie-breaker instead of random UUID.
- **refs.py:** move the SQLAlchemy `Row` import under `TYPE_CHECKING`; document/
  test `parse_ref` edge cases (`"@5"`, `"foo@"`).
- **diff():** skip the second SELECT when the first ref is missing; add tests
  for `None` body_md and cross-key diffs.
- **registry.load_entry_points:** wrap `ep.load()` failures with the entry-point
  name for actionable errors; unit-test the installed-after-local sticky branch.
