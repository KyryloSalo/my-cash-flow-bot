"""Review evidence retained by screenshot drafts without raw provider output."""
from copy import deepcopy


def screenshot_review_metadata(analysis) -> dict:
    return {
        "needs_review": bool(getattr(analysis, "needs_review", False)),
        "field_findings": deepcopy(list(getattr(analysis, "field_findings", ()) or ())),
        "field_evidence": deepcopy(getattr(analysis, "field_evidence", {}) or {}),
        "currency_candidates": deepcopy(list(getattr(analysis, "currency_candidates", ()) or ())),
    }


def unresolved_photo_currency(flow: dict) -> bool:
    """Return true when a screenshot draft lacks a positive, resolved currency."""
    if str(flow.get("draft_source") or flow.get("origin") or "").strip().lower() != "screenshot":
        return False
    tx = flow.get("tx")
    if isinstance(tx, dict) and not str(tx.get("currency") or "").strip():
        return True
    if any(not str(item.get("currency") or "").strip() for item in flow.get("items", ()) if isinstance(item, dict)):
        return True
    metadata = flow.get("draft_metadata") or {}
    findings = {str(value).strip().lower() for value in metadata.get("field_findings", ()) or ()}
    if bool(metadata.get("needs_review")) or findings.intersection({"currency_unknown", "currency_conflict"}):
        return True
    return False


def photo_review_warning(flow: dict) -> str:
    metadata = flow.get("draft_metadata") or {}
    findings = set(metadata.get("field_findings") or ())
    reason = "валюту не визначено однозначно" if findings.intersection({"currency_unknown", "currency_conflict"}) else "потрібне уточнення даних"
    return f"Не можу безпечно підтвердити скріншот: {reason}. Відкрийте чернетку в кабінеті для уточнення або скасуйте її."
