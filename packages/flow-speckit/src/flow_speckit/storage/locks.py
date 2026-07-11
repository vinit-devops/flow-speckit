"""Central registry of flow-speckit's Postgres advisory-lock class ids.

All advisory locks in flow-speckit use the two-arg
``pg_advisory_xact_lock(class_id, obj_id)`` form: the class id reserves a
subsystem namespace so per-object locks (e.g. ``hashtext(key)`` as obj_id)
cannot collide with advisory locks taken by other applications or other
flow-speckit subsystems sharing the same database. Runtime-only: advisory
locks never touch the schema, so no migration is involved.

Class ids are consecutive int4 values counting up from ASCII "FlSp".
"""

from __future__ import annotations

# Artifact engine namespace (artifacts/store.py): obj_id = hashtext(key)
# serializes concurrent creates per artifact key.
ARTIFACTS_LOCK_CLASS_ID = 0x466C_5370  # ASCII "FlSp"; fits in signed int4

# Workflow engine namespace (workflows/events.py): obj_id = hashtext(run_id)
# serializes event-log appends per run so seq allocation stays gapless.
WORKFLOWS_LOCK_CLASS_ID = 0x466C_5371  # "FlSp" + 1; fits in signed int4

# Fixed obj_id under WORKFLOWS_LOCK_CLASS_ID reserved for the scheduler-loop
# singleton (doc 03 §7: any worker may host the timer-firing loop, guarded by
# one advisory lock). No run_id hash can be constrained not to collide with a
# hand-picked value, so the scheduler uses pg_try_advisory_lock with this id
# on a dedicated connection rather than the xact-scoped per-run form.
SCHEDULER_LOCK_OBJECT_ID = 0x5343_4845  # ASCII "SCHE"; fits in signed int4
