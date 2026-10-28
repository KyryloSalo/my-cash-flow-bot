#!/usr/bin/env python3
"""Session-bound task contract and evidence index, not a deployment mechanism.

Records executed evidence; never invents or executes supplied command strings.
The independent delivery_gate.py additionally validates the final delivery receipt.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

MAX_ARTIFACT = 8 * 1024 * 1024
ARTIFACT_EXT = {".txt", ".log", ".json", ".md"}
EXCLUDED_DIRS = {".git", ".hermes", ".codex-deploy-keys", ".venv", ".venv-admin",
                 "venv", "__pycache__", ".pytest_cache", ".mypy_cache", "local_artifacts", "staticfiles"}


def now():
    return datetime.now(timezone.utc).isoformat()


def session_arg(value):
    value = value or os.environ.get("HERMES_SESSION_ID", "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise SystemExit("invalid or missing session id")
    return value


def protected(path):
    parts = [part.lower() for part in Path(path).parts]
    name = parts[-1] if parts else ""
    return (any(part in EXCLUDED_DIRS - {".hermes"} for part in parts)
            or name.startswith(".env") or name in {"auth.json", "config.yaml", "credentials.json"}
            or any(word in name for word in ("credential", "password", "secret", "token"))
            or Path(name).suffix in {".pem", ".key", ".sqlite3", ".zip"})


def safe_path(root, value, *, container=None):
    path = Path(value)
    path = path if path.is_absolute() else root / path
    if protected(path):
        raise SystemExit("protected path is not an artifact")
    boundary = (container or root).resolve()
    resolved = path.resolve()
    if not resolved.is_relative_to(boundary) or any(p.is_symlink() for p in (path, *path.parents) if p != root.parent):
        raise SystemExit("path escapes its allowed directory or is a symlink")
    return resolved


def path_for(root, sid):
    return safe_path(root, root / ".hermes/delivery" / sid / "task.json")


def git(root, *args, required=True):
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True)
    if required and result.returncode:
        raise SystemExit("git prerequisite failed (a repository with HEAD is required)")
    return result.stdout.decode("utf-8", errors="surrogateescape").strip()


def root_arg(value):
    location = Path(value or os.getcwd()).resolve()
    return Path(git(location, "rev-parse", "--show-toplevel")).resolve()


def changed_paths(root):
    paths = set(git(root, "diff", "--name-only", "-z", "HEAD").split("\0"))
    paths.update(git(root, "ls-files", "--others", "--exclude-standard", "-z").split("\0"))
    return sorted(p for p in paths if p and ".hermes" not in Path(p).parts and not protected(p))


def candidate(root):
    head = git(root, "rev-parse", "HEAD")
    paths = changed_paths(root)
    if not paths:
        return head
    digest = hashlib.sha256()
    for name in paths:
        path = safe_path(root, name)
        digest.update(name.encode("utf-8") + b"\0")
        digest.update(b"file\0" + path.read_bytes() if path.is_file() else b"deleted\0")
        digest.update(b"\0")
    return head + "+sha256:" + digest.hexdigest()


def load(root, sid):
    path = path_for(root, sid)
    if not path.is_file() or path.stat().st_size > MAX_ARTIFACT:
        raise SystemExit("task state missing or oversized")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeError):
        raise SystemExit("invalid task JSON") from None
    if not isinstance(data, dict) or data.get("schema_version") != 1 or data.get("session_id") != sid:
        raise SystemExit("task schema/session binding mismatch")
    return data


def save(root, sid, data):
    path = path_for(root, sid)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("task-" + uuid.uuid4().hex + ".tmp")
    safe_path(root, temporary)
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def evidence(root, sid, value):
    path = safe_path(root, value, container=path_for(root, sid).parent)
    if path.suffix.lower() not in ARTIFACT_EXT or not path.is_file() or not 0 < path.stat().st_size <= MAX_ARTIFACT:
        raise SystemExit("evidence must be a nonempty bounded .log/.txt/.json/.md in this task directory")
    return {"path": path.relative_to(root).as_posix(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def verify_evidence(root, sid, item):
    if not item or evidence(root, sid, item["path"]) != item:
        raise SystemExit("evidence changed or missing")


def emit(data):
    print(json.dumps(data, ensure_ascii=False, indent=2))


def gate_partition(data, gates):
    if not isinstance(gates, dict):
        raise SystemExit("invalid staged gate partition")
    groups = [gates.get(name) for name in ("integration", "production")]
    if any(not isinstance(group, list) or not group or
           any(not isinstance(value, str) or not value.strip() for value in group)
           for group in groups):
        raise SystemExit("invalid staged gate partition")
    supplied = groups[0] + groups[1]
    known = {requirement["id"] for requirement in data["requirements"]}
    if (len(supplied) != len(set(supplied)) or set(supplied) != known or
            any(not isinstance(gates.get(name), str) or not gates[name].strip()
                for name in ("approval", "cohort"))):
        raise SystemExit("staged gates must partition every requirement exactly once with plan approval and cohort")
    return set(groups[0]), set(groups[1])


def parser():
    ap = argparse.ArgumentParser(description=__doc__)
    subs = ap.add_subparsers(dest="cmd", required=True)
    def command(name):
        sub = subs.add_parser(name)
        sub.add_argument("--root")
        sub.add_argument("--session-id")
        return sub
    start = command("start")
    start.add_argument("--task-id", default=None)
    req = start.add_mutually_exclusive_group(required=True)
    req.add_argument("--request")
    req.add_argument("--request-file")
    start.add_argument("--requirement", action="append", required=True)
    start.add_argument("--kind", choices=["bugfix", "feature", "documentation"], default="feature")
    start.add_argument("--target", choices=["local", "production"], default="local")
    start.add_argument("--acceptance-command", default="")
    start.add_argument("--owner", default="local integrator")
    start.add_argument("--cohort", default="")
    start.add_argument("--review-required", dest="review_required", action="store_true", default=True)
    start.add_argument("--no-review-required", dest="review_required", action="store_false")
    start.add_argument("--review-waiver", default="")
    start.add_argument("--new-task", action="store_true")
    for name in ("status", "candidate", "snapshot", "preflight", "readiness"):
        command(name)
    gates = command("configure-gates")
    gates.add_argument("--integration-requirement", action="append", required=True)
    gates.add_argument("--production-requirement", action="append", required=True)
    gates.add_argument("--approval", required=True, help="Explicit authorization for the staged plan, NOT production Environment approval")
    gates.add_argument("--cohort", required=True)
    subs.choices["readiness"].add_argument("--stage", choices=["integration", "completion"], default="completion")
    for name in ("clarification", "requirement"):
        command(name).add_argument("text")
    check = command("record-check")
    check.add_argument("--name", required=True)
    check.add_argument("--kind", choices=["unit", "integration", "acceptance", "review", "runtime", "activation"], required=True)
    check.add_argument("--status", choices=["passed", "failed", "blocked"], required=True)
    check.add_argument("--candidate", required=True)
    check.add_argument("--environment", choices=["local", "test", "production"], required=True)
    check.add_argument("--exit-code", type=int, required=True)
    check.add_argument("--command", required=True)
    check.add_argument("--evidence", required=True)
    check.add_argument("--independent", action="store_true")
    check.add_argument("--reviewer", default="")
    accept = command("accept")
    accept.add_argument("--requirement-id", required=True)
    accept.add_argument("--check", required=True)
    repro = command("record-reproduction")
    repro.add_argument("--status", choices=["reproduced", "blocked", "not-applicable"], required=True)
    repro.add_argument("--command", required=True)
    repro.add_argument("--evidence", required=True)
    diag = command("record-diagnosis")
    diag.add_argument("--text", required=True)
    diag.add_argument("--result", choices=["disproved", "confirmed", "blocked"], required=True)
    command("reassess").add_argument("--note", required=True)
    release = command("record-release")
    release.add_argument("--active-revision", required=True)
    release.add_argument("--cohort", required=True)
    release.add_argument("--evidence", required=True)
    release.add_argument("--behavior-enabled", action="store_true")
    release.add_argument("--authoritative", action="store_true")
    return ap


def readiness(root, sid, data, stage="completion"):
    if data["candidate"] != candidate(root):
        raise SystemExit("candidate changed; snapshot and verify the new candidate")
    route = data["acceptance_command"].strip()
    if not route:
        raise SystemExit("named acceptance route required")
    preflight = data.get("preflight", {})
    if preflight.get("candidate") != data["candidate"] or preflight.get("acceptance_command") != route:
        raise SystemExit("current-candidate preflight required")
    if data.get("reassess_required"):
        raise SystemExit("reassess required after repeated disproved diagnoses or the same blocker")
    if data["kind"] == "bugfix":
        if data["reproduction"]["status"] != "reproduced":
            raise SystemExit("reproduction required for an explicit bugfix")
        verify_evidence(root, sid, data["reproduction"].get("evidence"))
    latest = {item["name"]: item for item in data["checks"]}
    def valid(check):
        return check["candidate"] == data["candidate"] and check["status"] == "passed" and check["exit_code"] == 0
    successful = [check for check in latest.values() if valid(check)]
    gates = data.get("gates")
    staged = "gates" in data
    if staged:
        ids = {r["id"] for r in data["requirements"]}
        integration, production = gate_partition(data, gates)
        required_ids = integration if stage == "integration" else ids
    else:
        if stage == "integration":
            raise SystemExit("integration stage requires an explicitly configured staged plan")
        required_ids = {r["id"] for r in data["requirements"]}
    route_envs = ({"local"} if stage == "integration" else {"local", "production"}) if staged else {data["target"]}
    for environment in sorted(route_envs):
        if not any(check["kind"] == "acceptance" and check["command"].strip() == route and
                   check["environment"] == environment for check in successful):
            raise SystemExit("named acceptance route has no passing " + environment + " target-environment evidence")
    required_kinds = {"acceptance"} | ({"unit"} if data["kind"] != "documentation" else set())
    if not required_kinds.issubset({check["kind"] for check in successful}):
        raise SystemExit("required checks incomplete")
    if data["review_required"] and not any(check["kind"] == "review" and check.get("independent") and check.get("reviewer") for check in successful):
        raise SystemExit("independent review missing")
    for check in successful:
        verify_evidence(root, sid, check.get("evidence"))
    for requirement in data["requirements"]:
        accepted = [latest.get(name) for name in requirement["evidence_ids"]]
        if requirement["id"] not in required_ids:
            continue
        envs = {"local"} if staged and requirement["id"] in integration else ({"production"} if staged else {data["target"]})
        if requirement["status"] != "passed" or not any(check and valid(check) and check["kind"] == "acceptance" and check["environment"] in envs for check in accepted):
            raise SystemExit("unverified " + data["target"] + " requirement: " + requirement["id"])
    if staged and stage == "integration":
        return "READY FOR INTEGRATION"
    if data["target"] == "production" or staged:
        release = data.get("deployment", {})
        kinds = {check["kind"] for check in successful if check["environment"] == "production"}
        cohort = gates["cohort"] if staged else data["requested_cohort"]
        if (not {"runtime", "activation"}.issubset(kinds) or release.get("active_revision") != data["candidate"]
                or not re.fullmatch(r"[0-9a-f]{40}", data["candidate"])
                or not release.get("behavior_enabled") or not release.get("result_authoritative")
                or release.get("cohort") != cohort or not cohort):
            raise SystemExit("production activation, exact revision and cohort readback required")
        verify_evidence(root, sid, release.get("evidence"))
    return "COMPLETE — " + ("PRODUCTION" if staged else data["target"].upper())


def main(argv=None):
    args = parser().parse_args(argv)
    root, sid = root_arg(args.root), session_arg(args.session_id)
    if args.cmd == "candidate":
        print(candidate(root))
        return 0
    if args.cmd == "start":
        request = args.request
        if request is None:
            path = safe_path(root, args.request_file)
            if path.suffix.lower() not in ARTIFACT_EXT or path.stat().st_size > 32768:
                raise SystemExit("request file must be a bounded text artifact")
            request = path.read_text(encoding="utf-8")
        if not request.strip() or any(not text.strip() for text in args.requirement):
            raise SystemExit("empty request/requirement")
        if not args.review_required and not args.review_waiver.strip():
            raise SystemExit("an explicit small-scope review waiver is required")
        path = path_for(root, sid)
        if path.exists():
            if not args.new_task:
                raise SystemExit("task exists; use --new-task to archive it")
            load(root, sid)
            archive = safe_path(root, path.with_name("task-" + uuid.uuid4().hex + ".json"))
            archive.write_bytes(path.read_bytes())
        data = {"schema_version": 1, "task_id": args.task_id or uuid.uuid4().hex, "session_id": sid,
                "request": request, "clarifications": [], "requirements": [], "target": args.target,
                "kind": args.kind, "base_sha": git(root, "merge-base", "HEAD", "origin/main", required=False) or git(root, "rev-parse", "HEAD"),
                "candidate": candidate(root), "integration_owner": args.owner, "changed_paths": changed_paths(root),
                "acceptance_command": args.acceptance_command, "review_required": args.review_required,
                "review_waiver": args.review_waiver, "requested_cohort": args.cohort,
                "reproduction": {"status": "missing"}, "checks": [], "blockers": [], "diagnoses": [],
                "reassessments": [], "reassess_required": False, "status": "in_progress", "created_at": now()}
        for text in args.requirement:
            data["requirements"].append({"id": "r" + str(len(data["requirements"]) + 1), "expected": text, "status": "unverified", "evidence_ids": []})
        save(root, sid, data)
        emit(data)
        return 0
    data = load(root, sid)
    if args.cmd == "status":
        emit(data)
        return 0
    if args.cmd == "configure-gates":
        plan = {"integration": args.integration_requirement, "production": args.production_requirement,
                "approval": args.approval, "cohort": args.cohort}
        gate_partition(data, plan)
        data["gates"] = plan
        data["status"] = "in_progress"
        save(root, sid, data)
        emit(data)
        return 0
    if args.cmd == "preflight":
        current = candidate(root)
        report = {"interpreter": sys.executable, "python": sys.version.split()[0], "root": str(root),
              "base_sha": data["base_sha"], "head_sha": git(root, "rev-parse", "HEAD"), "candidate": current,
              "dirty": "+sha256:" in current, "acceptance_command": data["acceptance_command"],
              "route_verified": False, "integration_owner": data["integration_owner"]}
        if data["acceptance_command"].strip():
            data["preflight"] = {"candidate": current, "acceptance_command": data["acceptance_command"].strip(),
                                 "interpreter": sys.executable, "checked_at": now()}
            save(root, sid, data)
        emit(report)
        return 0 if data["acceptance_command"].strip() else 2
    if args.cmd in {"clarification", "requirement"}:
        if not args.text.strip():
            raise SystemExit("empty clarification/requirement")
        if args.cmd == "clarification":
            data["clarifications"].append(args.text)
            for req in data["requirements"]:
                req.update(status="unverified", evidence_ids=[])
        else:
            data["requirements"].append({"id": "r" + str(len(data["requirements"]) + 1), "expected": args.text, "status": "unverified", "evidence_ids": []})
    elif args.cmd == "snapshot":
        data["candidate"] = candidate(root)
        data["changed_paths"] = changed_paths(root)
        for req in data["requirements"]:
            req.update(status="unverified", evidence_ids=[])
    elif args.cmd == "record-check":
        if args.candidate != candidate(root) or args.candidate != data["candidate"]:
            raise SystemExit("candidate mismatch; snapshot before recording new checks")
        if args.status == "passed" and args.exit_code != 0:
            raise SystemExit("a passing check must have exit code zero")
        if args.kind == "review" and args.status == "passed" and not (args.independent and args.reviewer.strip()):
            raise SystemExit("passing review must identify an independent reviewer")
        data["checks"].append({"name": args.name, "kind": args.kind, "status": args.status, "candidate": args.candidate,
                               "environment": args.environment, "exit_code": args.exit_code, "command": args.command,
                               "independent": args.independent, "reviewer": args.reviewer,
                               "evidence": evidence(root, sid, args.evidence), "recorded_at": now()})
    elif args.cmd == "accept":
        check = next((item for item in reversed(data["checks"]) if item["name"] == args.check), None)
        req = next((item for item in data["requirements"] if item["id"] == args.requirement_id), None)
        if not req or not check or check["kind"] != "acceptance" or check["status"] != "passed" or check["exit_code"] != 0 or check["candidate"] != candidate(root):
            raise SystemExit("accept requires a passing current-candidate acceptance check")
        verify_evidence(root, sid, check["evidence"])
        req["status"] = "passed"
        if args.check not in req["evidence_ids"]:
            req["evidence_ids"].append(args.check)
    elif args.cmd == "record-reproduction":
        data["reproduction"] = {"status": args.status, "command": args.command, "candidate": candidate(root),
                                "evidence": evidence(root, sid, args.evidence), "recorded_at": now()}
    elif args.cmd == "record-diagnosis":
        data["diagnoses"].append({"text": args.text, "result": args.result, "at": now()})
        since = data["diagnoses"][data["reassessments"][-1]["diagnosis_count"] if data["reassessments"] else 0:]
        disproved = sum(item["result"] == "disproved" for item in since)
        repeated_blocker = sum(item["result"] == "blocked" and item["text"] == args.text for item in since) >= 2
        data["reassess_required"] = disproved >= 2 or repeated_blocker
    elif args.cmd == "reassess":
        if not args.note.strip():
            raise SystemExit("reassessment must explain new evidence or needed access")
        data["reassessments"].append({"note": args.note, "diagnosis_count": len(data["diagnoses"]), "at": now()})
        data["reassess_required"] = False
    elif args.cmd == "record-release":
        if args.active_revision != candidate(root) or not re.fullmatch(r"[0-9a-f]{40}", args.active_revision):
            raise SystemExit("release readback requires an exact clean committed candidate SHA")
        data["deployment"] = {"active_revision": args.active_revision, "cohort": args.cohort,
                              "behavior_enabled": args.behavior_enabled, "result_authoritative": args.authoritative,
                              "checked_at": now(), "evidence": evidence(root, sid, args.evidence)}
    elif args.cmd == "readiness":
        result = readiness(root, sid, data, args.stage)
        data["status"] = "integration_ready" if args.stage == "integration" else "complete"
        save(root, sid, data)
        print(result)
        return 0
    data["status"] = "in_progress"
    save(root, sid, data)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print("INCOMPLETE: invalid state or unavailable artifact (" + type(exc).__name__ + ")", file=sys.stderr)
        raise SystemExit(2) from None
    except SystemExit as exc:
        if isinstance(exc.code, str):
            print("INCOMPLETE: " + exc.code, file=sys.stderr)
            raise SystemExit(2) from None
        raise
