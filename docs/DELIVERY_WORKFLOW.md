# Delivery workflow: working entrypoint and required CI

`ops/delivery.py` preserves the original request, clarifications, acceptance assertions and evidence at `.hermes/delivery/<session_id>/task.json`. The default-profile task-inspector reads this exact session-bound artifact when it can resolve the Git root; without it the inspector still uses genuine user messages. Task data and logs are ignored by Git, not shipped source.

The session receipt is a **local task gate**, not a deployment system. It never commits, pushes, deploys, approves an Environment, or overrides a production gate. The repository's required `validate` job automatically runs the CLI regression/normal-workspace acceptance tests and a real isolated PWA smoke before packaging. Exact-SHA validation, release packaging protections and manual production approval/readback remain unchanged. CI preserves browser logs in a SHA-bound artifact; a shell/onboarding smoke is not full financial-feature acceptance.

## Ordinary working folder

Use `python delivery.py <command>` from the repository, or from the usual outer project folder containing `code/`. The same checked-in launcher resolves `ops/delivery.py` and passes its actual Git root; it does not depend on a scratch worktree. The command options below are unchanged. The outer workspace's launcher and short AGENTS entrypoint rule are synchronized from reviewed `main`; existing unrelated changes in `code/` are preserved rather than reset.

## 1. Preserve the outcome; preflight before code

Run inside the task's Git worktree. Use the actual `HERMES_SESSION_ID`. Keep the complete agreed request in a sanitized `request.txt`; never paste credentials into it. Session identifiers start with a letter/digit and accept letters, digits, `.`, `-` and `_` (maximum 128 characters); path traversal is rejected.

```bash
python delivery.py start --session-id "$HERMES_SESSION_ID" --task-id task-name \
  --owner integrator --kind bugfix --target local --request-file request.txt \
  --requirement "Visible, testable outcome" \
  --acceptance-command "python ops/browser_acceptance_smoke.py --server"
python delivery.py preflight --session-id "$HERMES_SESSION_ID"
```

`kind` is explicit: `feature`, `bugfix`, or `documentation`, not guessed from English keywords. Preflight checks Git/runtime, source fingerprint and an explicit acceptance route, and persists that candidate-bound record. It does not claim that route passed. Execute the route early; unavailable device/provider access is a named BLOCKED acceptance, not a reason to invent a substitute. Repeat preflight for the final candidate after source changes.

Use `--new-task` only for a genuinely different request in the same session; it archives the previous task. `requirement "new assertion"` appends an r-id. `clarification "agreed clarification"` appends a clarification and invalidates previous acceptance links. Neither deletes the original request or previous requirements.

## 2. Reproduce; stop repeated blind fixes

For a bugfix, place the actual red run's sanitized log inside the session evidence directory, then record it:

```bash
python ops/delivery.py record-reproduction --session-id "$HERMES_SESSION_ID" \
  --status reproduced --command "actual failing scenario command" \
  --evidence ".hermes/delivery/$HERMES_SESSION_ID/red.log"
```

Missing reproduction or `blocked`/`not-applicable` is a blocker for a bugfix, never a passing assertion. Record hypotheses with `record-diagnosis --text ... --result disproved` (or `blocked`, `confirmed`). Two disproved diagnoses since the last reassessment, or the same blocker twice, require `reassess --note ...`. History is preserved. This gate cannot prevent arbitrary manual edits outside the CLI; agents must run the workflow.

## 3. Bind real checks to the final source

After source changes, run `snapshot`, then `candidate`. The candidate includes HEAD plus tracked dirty and nonignored untracked source bytes. A new commit is not required to invalidate stale evidence. Credentials/generated paths and `.hermes/delivery/` are excluded. Evidence files must be nonempty sanitized `.log`, `.json`, `.txt`, or `.md` files inside this session directory; source files are not evidence.

```bash
python ops/delivery.py snapshot --session-id "$HERMES_SESSION_ID"
CANDIDATE=$(python delivery.py candidate --session-id "$HERMES_SESSION_ID")
# Execute the actual checks; only then record their real exit/status and log.
python ops/delivery.py record-check --session-id "$HERMES_SESSION_ID" \
  --name unit --kind unit --environment local --status passed --exit-code 0 \
  --candidate "$CANDIDATE" --command "actual test command" \
  --evidence ".hermes/delivery/$HERMES_SESSION_ID/unit.log"
```

Record user-scenario checks with `--kind acceptance --environment local`; unit/backend CI is not PWA or provider acceptance. Link every requirement explicitly with `accept --requirement-id r1 --check scenario-name`. A new attempt for the same check name supersedes its previous attempt; old logs remain history. Source changes, log tampering, missing acceptance rows, and incomplete review block readiness.

Record substantial independent review with `--kind review --reviewer reviewer-id --independent` and the real review log. For a genuinely tiny low-risk task only, `start --no-review-required --review-waiver "reason"` is explicit, never the default. A boolean is not independent review by itself: give the reviewer the original requirements and exact source/evidence.

## 4. Integrate and report the right state

```bash
python ops/delivery.py readiness --session-id "$HERMES_SESSION_ID"
python ops/delivery.py status --session-id "$HERMES_SESSION_ID"
```

Readiness reports `COMPLETE — LOCAL` only when the named route exists, current-candidate preflight is recorded, a passing target-environment acceptance check records that exact route command, every requirement has current target-environment acceptance, required unit checks and independent review pass, reproduction/reassessment gates pass, and all evidence hashes still match. The CLI does not interpret whether a log tells the truth: exercised scenarios and source review remain mandatory.

### Evidence-before-claims verification

The repository CLI and direct verification of its evidence are the active local verification process. Apply `verification-before-completion` when that skill is available; the checks below remain mandatory without any agent-specific skill. The intentionally removed custom `delivery-verification` skill is neither an installed executable nor an additional required receipt gate. Do not restore it or add a parallel receipt system to satisfy an obsolete reference.

Before claiming completion or integrating a candidate:

1. Compare the complete original request, appended clarifications and every requirement with the actual observed outcomes. Do not reduce the contract to the implementation or treat a passing test count as acceptance.
2. Inspect the exact final candidate's snapshot and preflight, then read the real sanitized logs, commands, exit codes and explicit acceptance links for every requirement. For a bugfix, also verify the original failing reproduction and required reassessment history.
3. Verify that the named user-scenario command actually ran in the required environment and exercised the requested behavior. PWA work requires browser evidence; shell/onboarding smoke is not finance acceptance, and local evidence is not production evidence.
4. Inspect the independent review of that same candidate against the original requirements and evidence. Resolve blocking findings; a reviewer flag alone is not a review.
5. Run `readiness` and read its full result. Missing, failed, stale, tampered, blocked or incomplete evidence remains a blocker, even if another check is green. Reuse unchanged valid evidence; rerun checks invalidated by a source or scope change.
6. Before merge, read back the PR's exact head, required CI and review status. After merge, read back `state`, `mergedAt` and the merge SHA. Production still requires the separate approved exact-SHA deployment and authoritative behavior/revision readback below.

Removing the obsolete skill reference does not waive any of these checks, change CLI readiness semantics, weaken required CI or branch protection, or change the production delivery flow.

Production tasks remain `INCOMPLETE` from local checks: they require separate production acceptance per requirement, runtime/activation evidence and `record-release` for the exact clean full SHA and agreed cohort with `--behavior-enabled --authoritative`. Only record that after real approved rollout/readback; this is an evidence index, not permission to skip release gates. Use the existing PR → protected main → exact-SHA validation artifact → production Environment approval → rollout → revision/behavior readback route. A validated artifact or green unit suite is not an active feature.

Keep one named integration owner and one final candidate. Reuse valid unchanged evidence, not stale evidence after edits. Do not load unrelated histories, inspect all worktrees, refactor hotspot handlers, weaken CI or add new provider coupling just to complete a task. The separate [browser acceptance guide](BROWSER_ACCEPTANCE.md) describes what the local PWA smoke actually proves.


## Opt-in staged delivery gates

Use `configure-gates` after `start` and `preflight` to partition every requirement exactly once:

```bash
python ops/delivery.py configure-gates --session-id "$HERMES_SESSION_ID" \
  --integration-requirement r1 --production-requirement r2 \
  --approval "User authorized staged integration/completion gates" --cohort canary
python ops/delivery.py readiness --session-id "$HERMES_SESSION_ID" --stage integration
```

Both groups must be nonempty. Unknown, duplicate, empty, malformed, or later-uncovered requirement IDs fail closed, including malformed persisted plans. `--stage integration` is rejected unless a staged plan is explicitly configured; tasks without a plan keep their existing strict default behavior. Integration readiness uses the current candidate, preflight, named local acceptance, unit/review evidence, evidence hashes, reproduction, and reassessment gates, and reports `READY FOR INTEGRATION` without claiming completion. The default `readiness` stage remains `completion` for backward compatibility.

On a staged plan, completion requires local acceptance for integration requirements and production acceptance for production requirements, the named route in both environments, plus production runtime/activation and authoritative exact clean 40-character SHA/cohort readback. The explicitly configured cohort is authoritative for that staged plan even when the original task targeted local verification; successful final readiness reports `COMPLETE — PRODUCTION`, never local completion. Repeated `accept` calls append check IDs so local links remain available for completion.

`--approval` records the human's authorization to split the requirement gates only. It is **not** production Environment approval or evidence that deployment occurred. The CLI never approves, commits, merges, or deploys; the protected GitHub production approval, exact-SHA validation artifact, deployment and external readback remain separate mandatory steps. Gate configuration changes sequencing, not the original outcomes or source candidate: existing valid candidate-bound evidence may be reused, but cannot satisfy newly added requirements or changed source. Do not present synthetic CLI release fixtures as actual production evidence.
