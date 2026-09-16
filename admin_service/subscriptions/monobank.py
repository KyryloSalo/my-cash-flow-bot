from __future__ import annotations

import base64
import hashlib
import json
from functools import lru_cache
from urllib import error, request
from urllib.parse import quote

from django.conf import settings


class MonobankAPIError(Exception):
    pass


def _request_json(path: str, payload: dict | None = None, *, method: str | None = None) -> dict:
    if not settings.MONO_MERCHANT_TOKEN:
        raise MonobankAPIError("MONO_MERCHANT_TOKEN is not configured.")

    body = None if payload is None else json.dumps(payload).encode("utf-8")
    http_method = method or ("POST" if payload is not None else "GET")
    req = request.Request(
        f"{settings.MONO_API_BASE_URL.rstrip('/')}{path}",
        data=body,
        headers={
            "X-Token": settings.MONO_MERCHANT_TOKEN,
            "Content-Type": "application/json",
        },
        method=http_method,
    )
    try:
        with request.urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8")
            if not raw.strip():
                return {}
            return json.loads(raw)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise MonobankAPIError(raw or str(exc)) from exc
    except error.URLError as exc:
        raise MonobankAPIError(str(exc)) from exc


def create_invoice(payload: dict) -> dict:
    return _request_json("/api/merchant/invoice/create", payload)


def charge_wallet_payment(payload: dict) -> dict:
    return _request_json("/api/merchant/wallet/payment", payload)


def cancel_invoice(payload: dict) -> dict:
    return _request_json("/api/merchant/invoice/cancel", payload)


def fetch_invoice_status(invoice_id: str) -> dict:
    normalized_invoice_id = str(invoice_id or "").strip()
    if not normalized_invoice_id:
        raise MonobankAPIError("invoiceId is required.")
    encoded_invoice_id = quote(normalized_invoice_id, safe="")
    return _request_json(f"/api/merchant/invoice/status?invoiceId={encoded_invoice_id}")


def fetch_wallet_cards(wallet_id: str) -> dict:
    normalized_wallet_id = str(wallet_id or "").strip()
    if not normalized_wallet_id:
        raise MonobankAPIError("walletId is required.")
    encoded_wallet_id = quote(normalized_wallet_id, safe="")
    return _request_json(f"/api/merchant/wallet?walletId={encoded_wallet_id}")


def delete_wallet_card(card_token: str) -> dict:
    normalized_card_token = str(card_token or "").strip()
    if not normalized_card_token:
        raise MonobankAPIError("cardToken is required.")
    encoded_card_token = quote(normalized_card_token, safe="")
    return _request_json(f"/api/merchant/wallet/card?cardToken={encoded_card_token}", method="DELETE")


def fetch_pubkey() -> str:
    data = _request_json("/api/merchant/pubkey")
    return str(data.get("key") or "")


def _load_webhook_verify_key(pubkey_base64: str):
    try:
        import ecdsa
    except ImportError as exc:  # pragma: no cover
        raise MonobankAPIError("ecdsa is required for Monobank webhook verification.") from exc

    normalized_pubkey = str(pubkey_base64 or "").strip()
    if not normalized_pubkey:
        raise MonobankAPIError("Monobank public key is empty.")

    try:
        pubkey_pem = base64.b64decode(normalized_pubkey).decode("utf-8")
        return ecdsa.VerifyingKey.from_pem(pubkey_pem)
    except Exception as exc:  # pragma: no cover
        raise MonobankAPIError("Invalid Monobank public key.") from exc


@lru_cache(maxsize=1)
def _cached_webhook_verify_key():
    return _load_webhook_verify_key(fetch_pubkey())


def _get_webhook_verify_key(*, refresh: bool = False):
    if refresh:
        _cached_webhook_verify_key.cache_clear()
    return _cached_webhook_verify_key()


def _verify_signature_with_key(verify_key, *, body: bytes, signature: str) -> bool:
    try:
        import ecdsa
    except ImportError as exc:  # pragma: no cover
        raise MonobankAPIError("ecdsa is required for Monobank webhook verification.") from exc

    try:
        signature_bytes = base64.b64decode(signature)
        return bool(
            verify_key.verify(
                signature_bytes,
                body,
                sigdecode=ecdsa.util.sigdecode_der,
                hashfunc=hashlib.sha256,
            )
        )
    except ecdsa.BadSignatureError:
        return False
    except Exception:
        return False


def verify_webhook_signature(*, body: bytes, signature: str | None) -> bool:
    if not signature:
        return False
    verify_key = _get_webhook_verify_key()
    if _verify_signature_with_key(verify_key, body=body, signature=signature):
        return True

    refreshed_key = _get_webhook_verify_key(refresh=True)
    return _verify_signature_with_key(refreshed_key, body=body, signature=signature)
