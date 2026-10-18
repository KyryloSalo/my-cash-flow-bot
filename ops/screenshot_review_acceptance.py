from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "admin_service/miniapp/templates/miniapp/index.html"
VIEWS = ROOT / "admin_service/miniapp/views.py"


def main() -> int:
    html = TEMPLATE.read_text(encoding="utf-8")
    views = VIEWS.read_text(encoding="utf-8")
    checks = {
        "localized dismiss control": "aiImageBatchDismiss" in html and "Закрити перевірку" in html,
        "localized review reason": "aiImageBatchReviewReason" in html and "excluded_status_or_type" not in html,
        "dismiss endpoint clears persisted batch": 'action == "dismiss"' in views and "AI_IMAGE_BATCH_SESSION_KEY" in views,
        "same-file retry resets picker": 'image.input.value = ""' in html and "syncAiImageFileName" in html,
        "explicit confirmation remains": "confirmTransactionDraft" in html and "transaction_confirm" in views,
        "scrollable review list styling": "ai-image-batch-list" in html,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        for name in failed:
            print(f"FAIL {name}")
        return 1
    result = subprocess.run(
        [sys.executable, "-m", "unittest", "admin_service.miniapp.tests_unit.MiniAppViewUnitTests.test_ai_image_batch_item_can_be_reviewed_or_skipped_without_write", "admin_service.miniapp.tests_unit.MiniAppViewUnitTests.test_ai_image_batch_can_be_dismissed_without_financial_write"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    print(result.stdout, end="")
    print(result.stderr, end="")
    if result.returncode:
        return result.returncode
    for name in checks:
        print(f"PASS {name}")
    print("PASS static/unit preconditions; real browser acceptance is ops/screenshot_review_browser_acceptance.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
