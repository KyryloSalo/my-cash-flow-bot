"""Signed, explicit human review contract for recognition suggestions."""
import hashlib
import json
import uuid
from decimal import Decimal, InvalidOperation
from datetime import date
from django.core import signing
from django.utils import timezone

TTL_SECONDS = 3600
FIELDS = ("amount", "currency", "transaction_date", "kind", "account_id", "category_id")
_SELECT_FIELDS = {"kind", "account_id", "category_id"}
_REQUIRED = FIELDS
REVIEWED_FIELDS = FIELDS + ("comment", "receipt_items", "receipt_total", "receipt_resolution")
_BLOCKING_CODES = {"status_conflict", "type_conflict", "document_conflict", "pending", "declined", "refund", "transfer"}


class ReviewError(Exception):
    def __init__(self, code: str, message: str):
        self.code, self.message = code, message
        super().__init__(message)


def _safe_error(code, message):
    raise ReviewError(code, message)


def _identity(binding):
    if not isinstance(binding, dict) or not binding:
        _safe_error("invalid_identity", "Review identity is invalid.")
    return json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _now_ts():
    return int(timezone.now().timestamp())


def _field_value(suggestion, field):
    return suggestion.get(field)


def _fingerprint(fields, issues):
    raw = json.dumps({"fields": fields, "issues": issues}, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _canonical_amount(value):
    try:
        return format(Decimal(str(value)).quantize(Decimal("0.01")), "f")
    except (InvalidOperation, TypeError, ValueError):
        return value


def _canonical(value):
    if isinstance(value, (int, float, Decimal)):
        return _canonical_amount(value)
    if isinstance(value, str):
        text = value.strip()
        try:
            Decimal(text)
            return _canonical_amount(text)
        except (InvalidOperation, ValueError):
            pass
        try:
            return date.fromisoformat(text).isoformat()
        except ValueError:
            return text
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        result = {key: _canonical(item) for key, item in value.items()}
        if "amount" in result:
            result["amount"] = _canonical_amount(result["amount"])
        return result
    return value


def issue_review(binding: dict, suggestion: dict, *, attempt_id=None) -> dict:
    identity = _identity(binding)
    if not isinstance(suggestion, dict):
        _safe_error("invalid_suggestion", "Review suggestion is invalid.")
    source = suggestion.get("source") or "recognition"
    fields = {f: {"value": _field_value(suggestion, f), "state": "suggested", "source": source} for f in FIELDS}
    issues = []
    for field in _REQUIRED:
        if fields[field]["value"] in (None, ""):
            issues.append({"id": f"missing_{field}", "field": field, "code": "missing_required", "message": f"{field} is required."})
    if "kind" in (suggestion.get("missing_fields") or []) and not any(i["field"] == "kind" for i in issues):
        issues.append({"id": "missing_kind", "field": "kind", "code": "missing_required", "message": "Choose expense or income."})
    kind = suggestion.get("kind")
    if kind not in ("expense", "income"):
        issues.append({"id": "invalid_kind", "field": "kind", "code": "invalid_kind", "message": "Choose expense or income."})
    findings = suggestion.get("field_findings")
    if findings is not None and not isinstance(findings, (dict, list)):
        _safe_error("invalid_suggestion", "Review findings are invalid.")
    if isinstance(findings, dict):
        for field, finding in findings.items():
            code = finding.get("code") if isinstance(finding, dict) else finding
            if not isinstance(field, str) or not isinstance(code, str) or not code.strip():
                _safe_error("invalid_suggestion", "Review findings are invalid.")
            message = code
            if not isinstance(finding, dict) and code not in _BLOCKING_CODES:
                code = "field_finding"
            issues.append({"id": f"finding_{field}", "field": field, "code": code, "message": message})
    elif isinstance(findings, list):
        for index, finding in enumerate(findings):
            code = finding.get("code") if isinstance(finding, dict) else finding
            if not isinstance(code, str) or not code.strip():
                _safe_error("invalid_suggestion", "Review findings are invalid.")
            field = finding.get("field", "") if isinstance(finding, dict) else next((f for f in FIELDS if code.startswith(f + "_")), "")
            if not isinstance(field, str):
                _safe_error("invalid_suggestion", "Review findings are invalid.")
            issues.append({"id": f"finding_{index}", "field": field, "code": code, "message": code})
    if suggestion.get("needs_review") or suggestion.get("needs_review_bool"):
        issues.append({"id": "needs_review", "field": "", "code": "needs_review", "message": "Review this recognition result."})
    for n, warning in enumerate(suggestion.get("arithmetic_warnings") or []):
        issues.append({"id": f"arithmetic_{n}", "field": "amount", "code": "arithmetic_warning", "message": str(warning)})
    if source == "receipt" and not any(i["code"] == "receipt_review" for i in issues):
        issues.append({"id": "receipt_review", "field": "", "code": "receipt_review", "message": "Confirm the receipt details."})
    review_id = str(uuid.uuid4())
    snapshot = {"version": 1, "review_id": review_id, "review_nonce": uuid.uuid4().hex, "attempt_id": attempt_id, "identity": identity,
                "issued_at": _now_ts(), "fields": fields, "issues": issues, "source": source}
    snapshot["fingerprint"] = _fingerprint(fields, issues)
    token = signing.dumps(snapshot, salt="recognition-review-v1", compress=True)
    return {"version": 1, "review_id": review_id, "review_nonce": snapshot["review_nonce"], "attempt_id": attempt_id, "source_fingerprint": snapshot["fingerprint"], "token": token, "fields": fields, "issues": issues}


def _equal(field, expected, submitted):
    if field in _SELECT_FIELDS:
        return str(expected) == str(submitted)
    return _canonical(expected) == _canonical(submitted)


def resolve_review(binding: dict, payload: dict, *, server_context=None) -> dict:
    if not isinstance(payload, dict) or "recognition_review" not in payload:
        return {}
    review = payload.get("recognition_review")
    if not isinstance(review, dict) or not isinstance(review.get("token"), str) or len(review["token"]) > 8192:
        _safe_error("invalid_token", "Review token is invalid.")
    try:
        snapshot = signing.loads(review["token"], salt="recognition-review-v1", max_age=TTL_SECONDS)
    except signing.SignatureExpired:
        _safe_error("expired_token", "Review token has expired.")
    except (signing.BadSignature, ValueError, TypeError):
        _safe_error("invalid_token", "Review token is invalid.")
    if snapshot.get("identity") != _identity(binding):
        _safe_error("identity_mismatch", "Review identity does not match.")
    expected_attempt = payload.get("recognition_attempt_id")
    if expected_attempt is None and isinstance(review, dict):
        expected_attempt = review.get("recognition_attempt_id")
    if snapshot.get("attempt_id") != expected_attempt:
        _safe_error("attempt_mismatch", "Review does not match this recognition attempt.")
    if server_context is not None:
        if (server_context.get("attempt_id") != snapshot.get("attempt_id") or
                server_context.get("source_fingerprint") != snapshot.get("fingerprint") or
                server_context.get("review_nonce") != snapshot.get("review_nonce")):
            _safe_error("stale_review", "Review is not the active server recognition attempt.")
        if server_context.get("consumed"):
            _safe_error("replayed_review", "Review has already been resolved.")
    if "review_id" in review and review.get("review_id") != snapshot.get("review_id"):
        _safe_error("invalid_payload", "Review identity is invalid.")
    if snapshot.get("fingerprint") != _fingerprint(snapshot.get("fields"), snapshot.get("issues")):
        _safe_error("invalid_token", "Review token is invalid.")
    acknowledgements = review.get("acknowledgements")
    values = review.get("values")
    if not isinstance(acknowledgements, list) or any(not isinstance(i, str) for i in acknowledgements) or not isinstance(values, dict):
        _safe_error("invalid_payload", "Review payload is invalid.")
    issue_ids = [i["id"] for i in snapshot.get("issues", [])]
    if len(acknowledgements) != len(set(acknowledgements)):
        _safe_error("invalid_acknowledgement", "Acknowledgements must exactly match review issues.")
    if not set(issue_ids).issubset(set(acknowledgements)):
        _safe_error("missing_acknowledgement", "All review issues must be acknowledged.")
    if set(acknowledgements) != set(issue_ids):
        _safe_error("invalid_acknowledgement", "Acknowledgements must exactly match review issues.")
    if any(str(issue.get("code")) in _BLOCKING_CODES for issue in snapshot.get("issues", [])):
        _safe_error("blocking_finding", "This recognition finding requires a safe manual resolution.")
    for field in REVIEWED_FIELDS:
        current = payload.get(field, "items") if field == "receipt_resolution" else payload.get(field)
        if field in FIELDS and field not in values:
            _safe_error("missing_value", "All reviewed values must be submitted.")
        if field not in FIELDS and field in payload and field not in values:
            _safe_error("missing_value", "All reviewed values must be submitted.")
        submitted = values.get(field, "items") if field == "receipt_resolution" else values.get(field)
        if not _equal(field, current, submitted):
            _safe_error("stale_values", "Review values no longer match the current transaction.")
    audit_fields = {f: {"baseline": _canonical(snapshot["fields"][f]["value"]), "value": _canonical(values.get(f, payload.get(f)))} for f in FIELDS}
    return {"version": 1, "resolved_issues": snapshot["issues"], "fields": audit_fields, "source": snapshot.get("source")}
