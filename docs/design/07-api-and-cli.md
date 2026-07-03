# 07 — API & CLI

> The CLI is the product surface for v0.1–0.3; the REST/SSE server (v0.4 extra) reuses
> the same engine and models. This doc specifies the CLI as the literal wedge-demo
> transcript, then the HTTP surface.

## 1. Command tree

```
flow-speckit
├── init                     # set up .flow-speckit/ (embedded pg), flow-speckit.toml, detect repo
├── run <workflow> [--idea|--input k=v] [--auto-approve] [--backend NAME] [--detach]
├── resume <run-id>
├── runs   list | show <id> [--events] [--follow] | cancel <id>
├── gates  list | approve <run-id> <gate> [--comment] | reject <run-id> <gate> --comment
├── artifacts  list [--type] | show <ref> | versions <key> | diff <ref-a> <ref-b>
├── trace <artifact-ref | pr-url>
├── skills list | workflows list | backends list
├── import <path>            # .sdlc/ files → store (v0.2)
├── gc                       # orphaned worktrees, stale claims
├── server | worker | token create   # v0.4
└── --version | doctor       # doctor: db reachable, backends available, keys present
```

## 2. The wedge transcript (v0.1 acceptance script)

Every line below must work exactly as shown; this transcript is the E2E test.

```console
$ pip install flow-speckit
$ cd ~/code/reports-app
$ flow-speckit init
✓ Created flow-speckit.toml
✓ Embedded PostgreSQL initialized at .flow-speckit/pg
✓ Git repository detected: reports-app (branch: main)
✓ Backend available: claude-code (claude 2.x, authenticated)
  Run `flow-speckit doctor` anytime to re-check.

$ flow-speckit run feature --idea "Add CSV export to the reports page"
▶ Run 018f3c2a started — workflow feature@1

  [1/7] frame                    ✓ FrameBrief product/csv-export/brief@1     $0.04
  [2/7] brief_approval           ⏸ GATE — waiting for role:product

  Review:  flow-speckit artifacts show product/csv-export/brief@1
  Approve: flow-speckit gates approve 018f3c2a brief_approval

$ flow-speckit artifacts show product/csv-export/brief@1
# Frame Brief: CSV export for reports page
Problem: Users can view report tables but cannot take data into
spreadsheets ...
Success criteria: ...

$ flow-speckit gates approve 018f3c2a brief_approval --comment "Scope OK, keep it minimal"
✓ Gate brief_approval approved by vinit — run continuing

  [3/7] technical_design         ✓ TechnicalDesign design/csv-export@1       $0.31
  [4/7] design_approval          ⏸ GATE — waiting for role:eng-lead

$ flow-speckit gates approve 018f3c2a design_approval
✓ Gate design_approval approved by vinit — run continuing

  [5/7] task_planning            ✓ TaskPlan plan/csv-export@1                $0.12
  [6/7] implement (claude-code)  ⠸ running in worktree .flow-speckit/wt/018f3c2a…

# ——— durability money shot ———
$ kill -9 <flow-speckit-pid>
$ flow-speckit resume 018f3c2a
▶ Resuming run 018f3c2a — replaying 5 completed steps, continuing at [6/7] implement
  [6/7] implement (claude-code)  ✓ CodeChange change/csv-export@1  3 commits  $2.84
  [7/7] code_review + open_pr    ✓ ReviewReport · PR #142 opened

✔ Run 018f3c2a completed — total cost $3.31
  https://github.com/acme/reports-app/pull/142

# ——— lineage money shot ———
$ flow-speckit trace https://github.com/acme/reports-app/pull/142
PR #142  Add CSV export to the reports page
└─ change/csv-export@1        implement (claude-code)          $2.84
   └─ plan/csv-export@1       task_planning                    $0.12
      └─ design/csv-export@1  technical_design                 $0.31
         ├─ approved by vinit  2026-07-03 14:12  "—"
         └─ product/csv-export/brief@1  frame                  $0.04
            └─ approved by vinit 2026-07-03 14:03 "Scope OK, keep it minimal"

# ——— accountability money shot ———
$ flow-speckit runs show 018f3c2a --events
seq  event            detail
1    run_started      feature@1 by vinit
2    step_started     frame (skill)
3    step_completed   frame → product/csv-export/brief@1  $0.04  3.2s
4    gate_opened      brief_approval  approvers=[role:product]
5    gate_resolved    approved by vinit "Scope OK, keep it minimal"
...
```

UX rules: every pause prints the exact command to continue; every artifact line is a
copy-pasteable ref; `--detach` + `runs show --follow` replace the inline view for long
runs; exit codes — 0 success, 3 waiting-on-gate (`--detach`), 4 failed, 5 cancelled.

## 3. REST API (v0.4, FastAPI, `flow-speckit[server]` extra)

Auth: bearer tokens (`flow-speckit token create`); single-tenant. OpenAPI generated from the
same Pydantic models — the TS client post-1.0 is generated, not written.

| Method & path | Purpose |
|---|---|
| `GET  /v1/workflows` · `GET /v1/workflows/{name}` | List/describe (input schema, steps, gates) |
| `POST /v1/runs` | Start (`{workflow, input}`) |
| `GET  /v1/runs` · `GET /v1/runs/{id}` | List (filter by status/workflow) / detail + steps + costs |
| `POST /v1/runs/{id}/cancel` · `POST /v1/runs/{id}/resume` | Lifecycle |
| `GET  /v1/runs/{id}/events` | **SSE** live event stream (see §4) |
| `GET  /v1/gates?status=open` | Pending approvals inbox |
| `POST /v1/runs/{id}/gates/{gate}/resolve` | `{decision, comment}` — actor from token |
| `GET  /v1/artifacts` · `GET /v1/artifacts/{ref}` | List/get (`?format=json\|md`) |
| `GET  /v1/artifacts/{ref}/lineage` · `GET /v1/artifacts/{key}/versions` | Graph / history |
| `GET  /v1/artifacts/diff?a=&b=` | Structured + text diff |

## 4. SSE event contract

`GET /v1/runs/{id}/events` (also `?since_seq=N` for catch-up) emits each
`workflow_events` row as:

```
event: step_completed
id: 6
data: {"run_id":"018f3c2a","seq":6,"event_type":"step_completed",
       "payload":{"step_key":"technical_design",
                  "result_ref":"design/csv-export@1",
                  "cost":{"tokens_in":9412,"tokens_out":2210,"usd":0.31},
                  "duration_ms":41890},
       "created_at":"2026-07-03T14:10:22Z"}
```

SSE `id` = event `seq`, so `Last-Event-ID` reconnection resumes losslessly. One event
schema everywhere: the CLI `--follow` view, CI bots, the v0.5 UI, and webhooks all
consume this same shape. SSE over WebSocket: one-directional is sufficient, proxies are
kind to it, and no client library is needed.
