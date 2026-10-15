"""Versioned, offline photo-recognition benchmark runner.

This runner evaluates stored extraction evidence through ``photo_pipeline``. It
never claims model accuracy: without a labelled corpus, accuracy is explicitly
reported as not computed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from bot.photo_pipeline import Evidence, ExtractionPass, reconcile_extractions

REPEAT_COUNT = 10


def _load(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validation_error(code: str, path: str, detail: str | None = None) -> ValueError:
    payload = {"code": code, "path": path}
    if detail:
        payload["detail"] = detail
    return ValueError(json.dumps(payload, sort_keys=True))


def _passes(raw: Any) -> tuple[ExtractionPass, ...]:
    if not isinstance(raw, list):
        raise _validation_error("invalid_extractions", "extractions", "must be a list")
    parsed = []
    for index, item in enumerate(raw):
        path = f"extractions[{index}]"
        if not isinstance(item, dict):
            raise _validation_error("invalid_pass", path, "must be an object")
        if not isinstance(item.get("source"), str) or not item["source"]:
            raise _validation_error("invalid_pass_source", f"{path}.source")
        evidence = item.get("evidence")
        if not isinstance(evidence, list):
            raise _validation_error("invalid_pass_evidence", f"{path}.evidence", "must be a list")
        parsed_evidence = []
        for e_index, e in enumerate(evidence):
            e_path = f"{path}.evidence[{e_index}]"
            if not isinstance(e, dict):
                raise _validation_error("invalid_evidence", e_path, "must be an object")
            required = ("id", "field", "raw_text", "value")
            if any(not isinstance(e.get(key), str) for key in required):
                raise _validation_error("invalid_evidence_fields", e_path)
            if "region" in e and e["region"] is not None and not isinstance(e["region"], str):
                raise _validation_error("invalid_evidence_region", f"{e_path}.region")
            parsed_evidence.append(Evidence(e["id"], e["field"], e["raw_text"], e["value"], e.get("region")))
        parsed.append(ExtractionPass(item["source"], tuple(parsed_evidence)))
    return tuple(parsed)


def _check_expected(result: Any, expected: dict[str, Any]) -> list[str]:
    failures = []
    if result.accepted != expected["accepted"]:
        failures.append("expected_accepted_mismatch")
    findings = set(result.findings)
    if set(expected["findings_include"]) - findings:
        failures.append("expected_findings_include_missing")
    if set(expected["findings_exclude"]) & findings:
        failures.append("expected_findings_exclude_present")
    return failures


def _fixture(root: Path, value: Any, path: str) -> Path:
    if not isinstance(value, str) or not value:
        raise _validation_error("invalid_fixture_path", path)
    candidate = Path(value)
    if candidate.is_absolute() or any(part == ".." for part in candidate.parts):
        raise _validation_error("unsafe_fixture_path", path)
    resolved_root = root.resolve()
    resolved = (root / candidate).resolve()
    try:
        resolved.relative_to(resolved_root)
    except ValueError:
        raise _validation_error("unsafe_fixture_path", path)
    return resolved


def _validate_expected(expected: Any, path: str) -> dict[str, Any]:
    if not isinstance(expected, dict):
        raise _validation_error("invalid_expected", path, "must be an object")
    if not isinstance(expected.get("accepted"), bool):
        raise _validation_error("invalid_expected_accepted", f"{path}.accepted")
    for key in ("findings_include", "findings_exclude"):
        values = expected.get(key)
        if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
            raise _validation_error("invalid_expected_findings", f"{path}.{key}")
    return expected


def run_benchmark(manifest_path: str | Path, *, repeat: int = REPEAT_COUNT) -> dict[str, Any]:
    if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat <= 0:
        raise _validation_error("invalid_repeat", "repeat", "must be a positive integer")
    manifest_path = Path(manifest_path)
    manifest = _load(manifest_path)
    if not isinstance(manifest, dict):
        raise _validation_error("invalid_manifest", "manifest", "must be an object")
    if manifest.get("version") != 1:
        raise _validation_error("unsupported_manifest_version", "manifest.version")
    cases = manifest.get("cases")
    if not isinstance(cases, list):
        raise _validation_error("invalid_manifest_cases", "manifest.cases", "must be a list")
    root = manifest_path.parent
    results = []
    for index, case in enumerate(cases):
        case_path = f"manifest.cases[{index}]"
        if not isinstance(case, dict):
            raise _validation_error("invalid_case", case_path, "must be an object")
        for key in ("id", "sha256"):
            if not isinstance(case.get(key), str) or not case[key]:
                raise _validation_error("invalid_case_field", f"{case_path}.{key}")
        expected = _validate_expected(case.get("expected"), f"{case_path}.expected")
        image = _fixture(root, case.get("image"), f"{case_path}.image")
        extraction = _fixture(root, case.get("extractions"), f"{case_path}.extractions")
        item = {"id": case["id"], "failures": [], "sha256_verified": False}
        if not image.is_file() or not extraction.is_file():
            item["failures"].append("missing corpus")
            item["error"] = "missing corpus fixture"
            results.append(item)
            continue
        actual_sha = _sha256(image)
        item["sha256"] = actual_sha
        item["sha256_verified"] = actual_sha == str(case.get("sha256", "")).lower()
        if not item["sha256_verified"]:
            item["failures"].append("sha256_mismatch")
            results.append(item)
            continue
        passes = _passes(_load(extraction))
        runs = [reconcile_extractions(passes) for _ in range(repeat)]
        first = runs[0]
        signatures = [(r.accepted, str(r.amount), r.currency, r.status, r.transaction_type, r.findings) for r in runs]
        item.update({"accepted": first.accepted, "findings": list(first.findings), "repeat_count": repeat,
                     "repeat_consistent": len(set(signatures)) == 1})
        if not item["repeat_consistent"]:
            item["failures"].append("repeat_inconsistent")
        item["failures"].extend(_check_expected(first, case.get("expected", {})))
        results.append(item)
    complete = bool(cases) and all(not item["failures"] for item in results)
    metrics = {
        "cases": len(cases),
        "completed_cases": sum("missing corpus" not in item["failures"] for item in results),
        "missing_corpus_cases": sum("missing corpus" in item["failures"] for item in results),
        "sha256_verified_cases": sum(item["sha256_verified"] for item in results),
        "repeat_consistent_cases": sum(item.get("repeat_consistent", False) for item in results),
        "accuracy": "not computed (offline evidence benchmark has no labelled real corpus)",
    }
    return {"status": "passed" if complete else "failed", "manifest": str(manifest_path), "suite": manifest.get("suite"), "metrics": metrics, "cases": results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--repeat", type=int, default=REPEAT_COUNT)
    args = parser.parse_args(argv)
    try:
        report = run_benchmark(args.manifest, repeat=args.repeat)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "accuracy": "not computed"}, indent=2))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
