"""Validate audit docs, not product acceptance. Optional --browser uses Playwright."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def check() -> dict:
    register = json.loads((HERE / "register.json").read_text(encoding="utf-8"))
    inventory = json.loads((HERE / "inventory.json").read_text(encoding="utf-8"))
    baseline = json.loads((HERE / "baseline.json").read_text(encoding="utf-8"))
    assert register["source_sha"] == inventory["source_sha"] == baseline["main_sha"]["result"]
    assert re.fullmatch(r"[0-9a-f]{40}", register["source_sha"])
    sections = ("flows", "invariants", "findings", "decisions", "backlog")
    ids = {name: {row["id"] for row in register[name]} for name in sections}
    for name in sections:
        assert len(ids[name]) == len(register[name]), f"Duplicate ID in {name}"
    for flow in register["flows"]:
        assert set(flow["invariant_ids"]) <= ids["invariants"], flow["id"]
        assert flow["acceptance_status"] == "unverified", "No product acceptance performed"
        assert flow["next_acceptance"] and flow["evidence"], flow["id"]
    for row in register["backlog"]:
        assert set(row["depends_on"]) <= ids["backlog"], row["id"]
        assert set(row["flow_ids"]) <= ids["flows"], row["id"]
        assert row["exit_criteria"] and row["owner_role"], row["id"]
        assert row["release_status"] == "not_started", "Proposal is not a release"
        assert row["priority"] in {"P0", "P1", "P2"}, row["id"]
    for row in register["decisions"]:
        assert set(row["needed_by"]) <= ids["backlog"], row["id"]
    dependencies = {r["id"]: r["depends_on"] for r in register["backlog"]}

    def walk(node: str, trail: frozenset[str]) -> None:
        assert node not in trail, f"Dependency cycle: {node}"
        for parent in dependencies[node]:
            walk(parent, trail | {node})

    for node in dependencies:
        walk(node, frozenset())

    anchors = set()
    for name in ("flows", "findings"):
        for row in register[name]:
            anchors.update(row["evidence"])
    for name in ("configuration_names", "django_routes", "telegram_handlers", "task_decorators"):
        for row in inventory[name]:
            for source in row.get("sources", []):
                anchors.add(f"{source['path']}:{source['line']}")
            if "path" in row and "line" in row:
                anchors.add(f"{row['path']}:{row['line']}")
    for anchor in anchors:
        filename, line = anchor.rsplit(":", 1)
        candidate = (HERE / filename) if filename == "baseline.json" else (ROOT / filename)
        assert candidate.is_file(), f"Missing anchor: {anchor}"
        assert 0 < int(line) <= len(candidate.read_text(encoding="utf-8").splitlines()), anchor
    assert not inventory["parse_errors"], inventory["parse_errors"]
    for row in inventory["configuration_names"]:
        assert row["name"] and re.fullmatch(r"[A-Z][A-Z0-9_]*", row["name"]), row
        assert not ({"value", "default", "secret_value", "password"} & set(row)), row["name"]
    expected_counts = {
        "configuration_names": len(inventory["configuration_names"]),
        "django_route_fragments": len(inventory["django_routes"]),
        "telegram_handler_registrations": len(inventory["telegram_handlers"]),
        "task_decorators": len(inventory["task_decorators"]),
    }
    assert register["counts"] == expected_counts, "Inventory counts drifted"
    links = 0
    all_ids = set().union(*ids.values())
    for doc in HERE.glob("*.md"):
        text = doc.read_text(encoding="utf-8")
        for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", text):
            parsed = urlsplit(target)
            if parsed.scheme or not parsed.path:
                continue
            assert (doc.parent / unquote(parsed.path)).exists(), f"Broken link: {doc.name}: {target}"
            links += 1
        for identifier in re.findall(r"\b(?:FLOW|INV|HEALTH|DEC)-[A-Z0-9]+\b", text):
            assert identifier in all_ids, f"Unknown ID in {doc.name}: {identifier}"
    html = (HERE / "dashboard.html").read_text(encoding="utf-8")
    embedded = re.search(r'<script id="snapshot" type="application/json">(.*?)</script>', html, re.S)
    assert embedded, "Dashboard must carry an explicit offline snapshot"
    assert json.loads(embedded.group(1)) == register, "Dashboard snapshot drifted"
    return {"status": "passed", "scope": "documentation_consistency_only",
            "source_sha": register["source_sha"], "local_links": links,
            "source_anchors": len(anchors), **{name: len(register[name]) for name in sections},
            **expected_counts, "product_acceptance_executed": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser", action="store_true", help="Exercise the local read-only dashboard")
    arguments = parser.parse_args()
    result = check()
    if arguments.browser:
        from browser_check import verify_browser

        result["browser"] = verify_browser(HERE, ROOT)
    print(json.dumps(result, ensure_ascii=False, indent=2))
