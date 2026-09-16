from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

import config
from parsing import TxDraft, parse_tx

logger = logging.getLogger("mcf.ai_tx")


@dataclass(frozen=True)
class AccountOption:
    id: int
    label: str


@dataclass(frozen=True)
class CategoryOption:
    id: int
    kind: str  # expense|income
    name: str
    aliases: list[str] | None = None


@dataclass(frozen=True)
class NormalizedTx:
    date: date
    type: str  # expense|income
    amount: float
    currency: str
    account_id: int | None
    category_id: int | None
    comment: str | None
    source: str  # rules|llm


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _best_substring_match(text: str, options: list[str]) -> str | None:
    t = _norm(text)
    if not t:
        return None

    best: str | None = None
    best_len = 0
    for opt in options:
        o = _norm(opt)
        if not o:
            continue
        if o in t and len(o) > best_len:
            best = opt
            best_len = len(o)
    return best


def _pick_account_id(text: str, accounts: list[AccountOption]) -> int | None:
    label = _best_substring_match(text, [a.label for a in accounts])
    if not label:
        return None
    for a in accounts:
        if a.label == label:
            return a.id
    return None


def _pick_category_id(text: str, categories: list[CategoryOption]) -> int | None:
    t = _norm(text)
    if not t:
        return None
    best_id: int | None = None
    best_len = 0
    for c in categories:
        for token in [c.name] + list(c.aliases or []):
            s = _norm(token)
            if not s:
                continue
            if s in t and len(s) > best_len:
                best_len = len(s)
                best_id = c.id
    return best_id


def _rules_first_draft(
    text: str,
    *,
    default_date: date,
    default_currency: str,
    default_type: str,
    accounts: list[AccountOption],
    categories: list[CategoryOption],
) -> tuple[TxDraft, int | None, int | None]:
    draft = parse_tx(text, default_date=default_date, default_currency=default_currency, default_type=default_type)
    account_id = _pick_account_id(text, accounts)
    category_id = _pick_category_id(text, categories)
    return draft, account_id, category_id


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


async def _llm_normalize(
    text: str,
    *,
    today: date,
    default_currency: str,
    accounts: list[AccountOption],
    categories: list[CategoryOption],
) -> dict[str, Any]:
    if not config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    account_labels = [a.label for a in accounts][:20]
    category_names = [c.name for c in categories][:25]

    system = (
        "You normalize short user finance messages into a minimal JSON.\n"
        "Return JSON only. No markdown. No extra keys.\n"
        "If a field is unknown, return empty string for strings and null for numbers.\n"
        "Do not invent accounts/categories outside the provided lists.\n"
    )

    user = (
        f"Today is {today.isoformat()}.\n"
        f"Default currency is {default_currency}.\n"
        f"Accounts: {_json_text(account_labels)}\n"
        f"Categories: {_json_text(category_names)}\n\n"
        f"Message: {_json_text(text)}\n\n"
        "Return JSON with keys:\n"
        "- date (YYYY-MM-DD)\n"
        "- type (expense|income)\n"
        "- amount (number)\n"
        "- currency (ISO like UAH/USD/EUR)\n"
        "- account_label (string)\n"
        "- category_name (string)\n"
        "- comment (string)\n"
    )

    headers = {"Authorization": f"Bearer {config.OPENAI_API_KEY}"}
    payload = {
        "model": config.OPENAI_TX_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "response_format": {"type": "json_object"},
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()

    content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
    if not content:
        raise RuntimeError("Empty LLM response")
    try:
        obj = json.loads(content)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"LLM returned non-JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise RuntimeError("LLM JSON is not an object")
    return obj


async def normalize_tx(
    text: str,
    *,
    today: date,
    default_currency: str,
    accounts: list[AccountOption],
    expense_categories: list[CategoryOption],
    income_categories: list[CategoryOption],
) -> NormalizedTx | None:
    """
    Cheap/fast strategy:
    - rules-first parse (amount/date/type/currency)
    - cheap substring match for account/category
    - LLM fallback only when missing account/category OR amount is missing
    """
    t = (text or "").strip()
    if not t:
        return None

    draft, account_id, category_id = _rules_first_draft(
        t,
        default_date=today,
        default_currency=default_currency,
        default_type="expense",
        accounts=accounts,
        categories=expense_categories + income_categories,
    )

    if draft.type == "transfer":
        return None

    categories = income_categories if draft.type == "income" else expense_categories

    if category_id is None:
        category_id = _pick_category_id(t, categories)

    if draft.amount is not None and (account_id is not None) and (category_id is not None):
        return NormalizedTx(
            date=draft.date,
            type=draft.type,
            amount=float(draft.amount),
            currency=draft.currency,
            account_id=account_id,
            category_id=category_id,
            comment=draft.comment,
            source="rules",
        )

    if not config.OPENAI_API_KEY:
        # Allow partial draft when we have at least amount/date/type/currency.
        if draft.amount is None:
            return None
        return NormalizedTx(
            date=draft.date,
            type=draft.type,
            amount=float(draft.amount),
            currency=draft.currency,
            account_id=account_id,
            category_id=category_id,
            comment=draft.comment,
            source="rules",
        )

    try:
        obj = await _llm_normalize(
            t,
            today=today,
            default_currency=default_currency,
            accounts=accounts,
            categories=categories,
        )
    except Exception as exc:
        logger.warning("LLM normalize failed: %s", exc)
        return None

    try:
        tx_type = _norm(str(obj.get("type") or ""))
        if tx_type not in {"expense", "income"}:
            return None

        amount_raw = obj.get("amount")
        if amount_raw is None:
            return None
        amount = float(amount_raw)

        currency = (str(obj.get("currency") or "")).strip().upper() or default_currency
        dt = (str(obj.get("date") or "")).strip() or today.isoformat()
        tx_date = date.fromisoformat(dt)

        account_label = str(obj.get("account_label") or "").strip()
        category_name = str(obj.get("category_name") or "").strip()

        picked_categories = income_categories if tx_type == "income" else expense_categories
        picked_accounts = accounts

        acc_id = None
        for a in picked_accounts:
            if a.label == account_label:
                acc_id = a.id
                break
        if acc_id is None:
            acc_id = _pick_account_id(t, picked_accounts)
        # Account/category may remain unknown: UI will ask user to pick.

        cat_id = None
        for c in picked_categories:
            if c.name == category_name or category_name in (c.aliases or []):
                cat_id = c.id
                break
        if cat_id is None:
            cat_id = _pick_category_id(t, picked_categories)
        # Account/category may remain unknown: UI will ask user to pick.

        comment = str(obj.get("comment") or "").strip()[:500] or None
        return NormalizedTx(
            date=tx_date,
            type=tx_type,
            amount=amount,
            currency=currency,
            account_id=acc_id,
            category_id=cat_id,
            comment=comment,
            source="llm",
        )
    except Exception:
        return None
