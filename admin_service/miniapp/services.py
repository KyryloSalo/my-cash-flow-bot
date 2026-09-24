from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from collections import defaultdict
import re

from django.conf import settings
from django.db import connection, transaction as db_transaction
from django.db.models import Q, Sum
from django.utils import timezone

from accounts.models import Account
from categories.models import Category
from gamification.producers import enqueue_transaction_created
from miniapp.fx import get_latest_rates
from miniapp.image_uploads import detect_heif_mime
from miniapp.models import AiTransactionDraft, SavingPromptSetting
from subscriptions.models import BillingProfile, PromoOffer, Subscription
from transactions.models import Debt, DebtPayment, Transaction
from users.models import TelegramUser, UserAdminState


ZERO = Decimal("0.00")
ONE_HUNDRED = Decimal("100.00")
FULL_ACCESS_SCOPES = {"personal_full", "family_full"}
PERSONAL_FULL_STATUSES = {"trial", "active", "paid", "manual", "lifetime"}
CATEGORY_COLORS = ["#2bd0aa", "#ff6290", "#ffbe52", "#78a3ff", "#6fe3d7"]
ASSET_ACCOUNT_TYPES = {"main", "cash", "savings", "deposit", "investment", "other"}
LIABILITY_ACCOUNT_TYPES = {"credit"}
INVESTMENT_TYPES = {"investment"}
GOAL_TYPES = {"savings", "deposit", "investment"}
SUPPORTED_LOCALES = {"uk", "en"}
SUPPORTED_NORMAL_TRANSACTION_KINDS = {"income", "expense"}
MINIAPP_TRANSACTION_SOURCE = "miniapp_manual"
CREDIT_ACCOUNT_TYPE = "credit"
LEGACY_ACCOUNT_TYPE_ALIASES = {"card": "main", "bank": "main", "crypto": "investment"}
SUPPORTED_ACCOUNT_TYPES = ASSET_ACCOUNT_TYPES | LIABILITY_ACCOUNT_TYPES | {"other"}
ACCOUNT_ACTIONS = {"create", "rename", "archive", "balance_correction", "goal_update"}
MINIAPP_ACCOUNT_SOURCE = "miniapp_account"
MINIAPP_BALANCE_CORRECTION_SOURCE = "miniapp_balance_correction"
MINIAPP_TRANSFER_SOURCE = "miniapp_transfer"
MINIAPP_DEBT_SOURCE = "miniapp_debt"
MINIAPP_BILLING_EXPENSE_SOURCE = "miniapp_billing_subscription"
BILLING_EXPENSE_DRAFT_SOURCE = "billing_subscription"
AI_TEXT_MAX_LENGTH = 500
AI_IMAGE_MAX_BYTES = 10 * 1024 * 1024
AI_IMAGE_BATCH_MAX_ITEMS = 50
AI_VOICE_MAX_BYTES = 10 * 1024 * 1024
CURRENCY_CODE_PATTERN = re.compile(r"^[A-Z]{3,10}$")
AI_TRANSFER_COMMAND_PATTERN = re.compile(
    r"(?i)\b(?:переказ\w*|перекин\w*|перев(?:ести|еди|ів|ела|од\w*)|transfer\w*)\b"
)
AI_TRANSFER_ROUTE_PATTERN = re.compile(r"(?is)\b(?:з|із|зі|від|from)\b.+\b(?:на|до|to)\b")
DEBT_ACTIONS = {"create", "repay", "edit", "close"}
ACTIVE_DEBT_STATUSES = {"active", "partially_paid"}
SUPPORTED_REMINDER_MODES = {"daily", "weekdays", "off"}
SUPPORTED_REMINDER_HOURS = {19, 20, 21, 22}
DEFAULT_REMINDER_MODE = "daily"
DEFAULT_REMINDER_HOUR = 21
DEFAULT_REMINDER_TIMEZONE = "Europe/Kyiv"
DEFAULT_BASE_CURRENCIES = ("UAH", "USD", "EUR", "TRY", "USDT")


class MiniAppTransactionError(ValueError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int = 400,
        extra: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.extra = extra or {}


def normalize_locale(value: str | None, *, default: str = "uk") -> str:
    normalized = str(value or "").strip().lower()
    if normalized.startswith("en"):
        return "en"
    if normalized.startswith("uk"):
        return "uk"
    return default


def locale_for_user(user: TelegramUser | None) -> str:
    return normalize_locale(getattr(user, "lang", None))


def mt(uk_text: str, en_text: str, locale: str | None = None) -> str:
    return en_text if normalize_locale(locale) == "en" else uk_text


@dataclass(slots=True)
class PeriodSelection:
    preset: str
    date_from: date
    date_to: date
    label: str


@dataclass(slots=True)
class MiniAppFinanceScope:
    type: str
    user_id: int
    family_id: int | None = None
    role: str | None = None

    @property
    def is_family(self) -> bool:
        return self.type == "family" and self.family_id is not None


@dataclass(slots=True)
class AggregatedMoneyResult:
    payload: dict[str, object]
    mode: str
    reason: str | None = None
    note: str | None = None


@dataclass(slots=True)
class ConvertedRowsResult:
    rows: list[dict[str, object]]
    mode: str
    reason: str | None = None
    note: str | None = None
    comparison_available: bool = True


def _quantize(value: Decimal | int | float | str | None) -> Decimal:
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _format_decimal(value: Decimal | int | float | str | None) -> str:
    amount = _quantize(value)
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    normalized = f"{amount:,.2f}".replace(",", " ").replace(".", ",")
    if normalized.endswith(",00"):
        normalized = normalized[:-3]
    return f"{sign}{normalized}"


def _currency_suffix(currency: str | None) -> str:
    normalized = str(currency or "").strip().upper() or "UAH"
    return {
        "UAH": "грн",
        "USD": "$",
        "EUR": "€",
        "PLN": "zł",
    }.get(normalized, normalized)


def _money_minor_units_to_label(value_minor: int | str | None, currency: str | None = "UAH") -> str:
    try:
        amount_minor = int(value_minor or 0)
    except (TypeError, ValueError):
        amount_minor = 0
    amount = Decimal(amount_minor) / Decimal("100")
    return money_payload(amount, currency)["display"]


def money_payload(value: Decimal | int | float | str | None, currency: str | None) -> dict[str, object]:
    amount = _quantize(value)
    normalized_currency = str(currency or "").strip().upper() or "UAH"
    suffix = _currency_suffix(normalized_currency)
    return {
        "value": f"{amount:.2f}",
        "currency": normalized_currency,
        "display": f"{_format_decimal(amount)} {suffix}".strip(),
        "is_mixed": False,
        "is_estimated": False,
        "note": None,
    }


def mixed_money_payload(*, display: str, note: str | None = None) -> dict[str, object]:
    return {
        "value": None,
        "currency": None,
        "display": display,
        "is_mixed": True,
        "is_estimated": False,
        "note": note,
    }


def unavailable_money_payload(*, currency: str | None, note: str) -> dict[str, object]:
    normalized_currency = str(currency or "").strip().upper() or "UAH"
    return {
        "value": None,
        "currency": normalized_currency,
        "display": "-",
        "is_mixed": True,
        "is_estimated": False,
        "note": note,
    }


def estimated_money_payload(
    value: Decimal | int | float | str | None,
    currency: str | None,
    *,
    note: str | None = None,
    is_mixed: bool = True,
) -> dict[str, object]:
    payload = money_payload(value, currency)
    payload["display"] = f"~ {payload['display']}"
    payload["is_mixed"] = is_mixed
    payload["is_estimated"] = True
    payload["note"] = note
    return payload


def _percent_change(current: Decimal, previous: Decimal) -> int | None:
    current = _quantize(current)
    previous = _quantize(previous)
    if previous == ZERO:
        return None
    return int(((current - previous) / previous * ONE_HUNDRED).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _normalize_currency(value: str | None, *, default: str = "UAH") -> str:
    return str(value or "").strip().upper() or default


def _unique_currencies(values: list[str], *, base_currency: str) -> list[str]:
    normalized_base = _normalize_currency(base_currency)
    return sorted(
        {
            _normalize_currency(value, default=normalized_base)
            for value in values
            if str(value or "").strip()
        }
    )


def _foreign_currencies(values: list[str], *, base_currency: str) -> list[str]:
    normalized_base = _normalize_currency(base_currency)
    return [
        currency
        for currency in _unique_currencies(values, base_currency=normalized_base)
        if currency != normalized_base
    ]


def _fx_snapshot_for_currencies(base_currency: str, currencies: list[str]) -> tuple[object | None, list[str], bool]:
    foreign_currencies = _foreign_currencies(currencies, base_currency=base_currency)
    if not foreign_currencies:
        return None, foreign_currencies, True
    try:
        return get_latest_rates(base_currency), foreign_currencies, True
    except Exception:
        return None, foreign_currencies, False


def _convert_amount_to_base(amount: Decimal | int | float | str | None, currency: str, base_currency: str, fx_snapshot) -> Decimal | None:
    if amount is None:
        return None
    normalized_base = _normalize_currency(base_currency)
    normalized_currency = _normalize_currency(currency, default=normalized_base)
    if normalized_currency == normalized_base:
        return _quantize(amount)
    if fx_snapshot is None:
        return None
    rate = fx_snapshot.rates.get(normalized_currency)
    if rate is None:
        return None
    return _quantize(Decimal(str(amount)) * Decimal(str(rate)))


def _fx_estimate_note(fx_snapshot, foreign_currencies: list[str], *, locale: str = "uk") -> str:
    date_suffix = f" на {fx_snapshot.date}" if fx_snapshot and getattr(fx_snapshot, "date", None) else ""
    currencies_note = ", ".join(foreign_currencies)
    if currencies_note:
        return mt(
            f"Орієнтовно за курсом НБУ{date_suffix}. Є валюти: {currencies_note}.",
            f"Estimated using the NBU rate{date_suffix}. Foreign currencies: {currencies_note}.",
            locale,
        )
    return mt(
        f"Орієнтовно за курсом НБУ{date_suffix}.",
        f"Estimated using the NBU rate{date_suffix}.",
        locale,
    )


def _fx_unavailable_note(base_currency: str, foreign_currencies: list[str], *, locale: str = "uk") -> str:
    currencies_note = ", ".join(foreign_currencies)
    if currencies_note:
        return mt(
            f"Орієнтовний підсумок у {base_currency} тимчасово недоступний: не вдалося отримати курс НБУ для {currencies_note}.",
            f"The estimated total in {base_currency} is temporarily unavailable: couldn't load the NBU rate for {currencies_note}.",
            locale,
        )
    return mt(
        f"Орієнтовний підсумок у {base_currency} тимчасово недоступний: не вдалося отримати курс НБУ.",
        f"The estimated total in {base_currency} is temporarily unavailable: couldn't load the NBU rate.",
        locale,
    )


def _merge_notes(*notes: str | None) -> str | None:
    unique: list[str] = []
    for note in notes:
        cleaned = str(note or "").strip()
        if cleaned and cleaned not in unique:
            unique.append(cleaned)
    if not unique:
        return None
    return " ".join(unique)


def _rows_to_money_legacy(rows: list[dict[str, object]], *, preferred_currency: str) -> dict[str, object]:
    cleaned: list[tuple[str, Decimal]] = []
    for row in rows:
        currency = str(row.get("currency") or "").strip().upper() or preferred_currency
        total = _quantize(row.get("total"))
        if total == ZERO:
            continue
        cleaned.append((currency, total))

    if not cleaned:
        return money_payload(ZERO, preferred_currency)

    if len(cleaned) == 1:
        currency, total = cleaned[0]
        return money_payload(total, currency)

    preferred_total = next((total for currency, total in cleaned if currency == preferred_currency), None)
    if preferred_total is not None:
        others = ", ".join(currency for currency, _ in cleaned if currency != preferred_currency)
        payload = money_payload(preferred_total, preferred_currency)
        payload["is_mixed"] = True
        payload["note"] = f"Є інші валюти: {others}" if others else None
        return payload

    currencies = ", ".join(currency for currency, _ in cleaned)
    return mixed_money_payload(display="Різні валюти", note=currencies)


def _rows_to_money(rows: list[dict[str, object]], *, preferred_currency: str) -> AggregatedMoneyResult:
    totals_by_currency: defaultdict[str, Decimal] = defaultdict(lambda: ZERO)
    for row in rows:
        currency = _normalize_currency(str(row.get("currency") or ""), default=preferred_currency)
        total = _quantize(row.get("total"))
        if total == ZERO:
            continue
        totals_by_currency[currency] += total

    if not totals_by_currency:
        return AggregatedMoneyResult(payload=money_payload(ZERO, preferred_currency), mode="full")

    normalized_base = _normalize_currency(preferred_currency)
    cleaned = sorted(totals_by_currency.items())
    foreign_currencies = [currency for currency, _ in cleaned if currency != normalized_base]
    if not foreign_currencies:
        total = sum((amount for _, amount in cleaned), ZERO)
        return AggregatedMoneyResult(payload=money_payload(total, normalized_base), mode="full")

    fx_snapshot, _, fx_available = _fx_snapshot_for_currencies(
        normalized_base,
        [currency for currency, _ in cleaned],
    )
    if not fx_available or fx_snapshot is None:
        note = _fx_unavailable_note(normalized_base, foreign_currencies)
        return AggregatedMoneyResult(
            payload=unavailable_money_payload(currency=normalized_base, note=note),
            mode="partial",
            reason="fx_unavailable",
            note=note,
        )

    converted_total = ZERO
    for currency, total in cleaned:
        converted = _convert_amount_to_base(total, currency, normalized_base, fx_snapshot)
        if converted is None:
            note = _fx_unavailable_note(normalized_base, foreign_currencies)
            return AggregatedMoneyResult(
                payload=unavailable_money_payload(currency=normalized_base, note=note),
                mode="partial",
                reason="fx_unavailable",
                note=note,
            )
        converted_total += converted

    note = _fx_estimate_note(fx_snapshot, foreign_currencies)
    return AggregatedMoneyResult(
        payload=estimated_money_payload(
            converted_total,
            normalized_base,
            note=note,
            is_mixed=len(cleaned) > 1,
        ),
        mode="partial",
        reason="fx_estimated",
        note=note,
    )


def _rows_to_usd_equivalent(rows: list[dict[str, object]]) -> AggregatedMoneyResult:
    cleaned: list[dict[str, object]] = []
    currencies: set[str] = set()
    direct_total = ZERO

    for row in rows:
        currency = _normalize_currency(str(row.get("currency") or ""), default="USD")
        total = _quantize(row.get("total"))
        if total == ZERO:
            continue
        cleaned.append({"currency": currency, "total": total})
        currencies.add(currency)
        if currency in {"USD", "USDT"}:
            direct_total += total

    if not cleaned:
        return AggregatedMoneyResult(payload=money_payload(ZERO, "USD"), mode="full")

    if currencies and currencies.issubset({"USD", "USDT"}):
        payload = money_payload(direct_total, "USD")
        payload["is_mixed"] = len(currencies) > 1
        return AggregatedMoneyResult(payload=payload, mode="full")

    return _rows_to_money(cleaned, preferred_currency="USD")


def _converted_queryset_rows(queryset, *, base_currency: str, extra_fields: tuple[str, ...] = ()) -> ConvertedRowsResult:
    fields = ["id", "date", "type", "amount", "currency", *extra_fields]
    raw_rows = [dict(row) for row in queryset.values(*fields).order_by("date", "id")]
    foreign_currencies = _foreign_currencies(
        [str(row.get("currency") or "") for row in raw_rows],
        base_currency=base_currency,
    )

    if not foreign_currencies:
        for row in raw_rows:
            row["base_amount"] = _quantize(row.get("amount"))
        return ConvertedRowsResult(rows=raw_rows, mode="full")

    fx_snapshot, _, fx_available = _fx_snapshot_for_currencies(
        base_currency,
        [str(row.get("currency") or "") for row in raw_rows],
    )
    if not fx_available or fx_snapshot is None:
        return ConvertedRowsResult(
            rows=[],
            mode="partial",
            reason="fx_unavailable",
            note=_fx_unavailable_note(_normalize_currency(base_currency), foreign_currencies),
            comparison_available=False,
        )

    converted_rows: list[dict[str, object]] = []
    for row in raw_rows:
        converted = _convert_amount_to_base(row.get("amount"), str(row.get("currency") or ""), base_currency, fx_snapshot)
        if converted is None:
            return ConvertedRowsResult(
                rows=[],
                mode="partial",
                reason="fx_unavailable",
                note=_fx_unavailable_note(_normalize_currency(base_currency), foreign_currencies),
                comparison_available=False,
            )
        row["base_amount"] = converted
        converted_rows.append(row)

    return ConvertedRowsResult(
        rows=converted_rows,
        mode="partial",
        reason="fx_estimated",
        note=_fx_estimate_note(fx_snapshot, foreign_currencies),
    )


def _normalize_failure_reason(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _is_soft_grace_reason(value: str | None) -> bool:
    normalized = _normalize_failure_reason(value)
    if not normalized:
        return False
    soft_markers = (
        "insufficient",
        "not enough",
        "low balance",
        "declin",
        "do not honor",
        "недостат",
        "не вистач",
        "брак кошт",
    )
    return any(marker in normalized for marker in soft_markers)


def _billing_access_mode(
    *,
    subscription_status: str,
    expires_at: datetime | None,
    grace_expires_at: datetime | None,
    soft_grace_active: bool = False,
) -> str:
    now = datetime.now(UTC)
    if expires_at is None:
        if subscription_status in {"manual", "lifetime"}:
            return "full"
        return "open"
    if expires_at >= now:
        return "full"
    if soft_grace_active and grace_expires_at and grace_expires_at >= now:
        return "full"
    if grace_expires_at and grace_expires_at >= now:
        return "read_only"
    return "blocked"


def _family_sponsor_has_access(family_id: int) -> bool:
    # Both the canonical family owner and their active owner membership must agree.
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT f.owner_user_id FROM families f
            JOIN family_members fm ON fm.family_id=f.id AND fm.user_id=f.owner_user_id
            WHERE f.id=%s AND f.status='active' AND fm.role='owner' AND fm.status='active'
        """, [family_id])
        row = cursor.fetchone()
    if row is None:
        return False
    owner_id = int(row[0])
    if UserAdminState.objects.filter(telegram_user_id=owner_id).filter(Q(is_blocked=True) | Q(status="banned")).exists():
        return False
    subscription = Subscription.objects.filter(user_id=owner_id).order_by("-created_at", "-id").first()
    if subscription is None:
        return False
    profile = BillingProfile.objects.filter(user_id=owner_id).first()
    soft = subscription.status == "expired" and _is_soft_grace_reason(profile.last_failure_reason if profile else "")
    return (
        _billing_access_mode(subscription_status=subscription.status, expires_at=subscription.expires_at, grace_expires_at=subscription.grace_expires_at, soft_grace_active=soft) == "full"
        and (subscription.status in PERSONAL_FULL_STATUSES or soft)
    )


def resolve_access(user: TelegramUser) -> dict[str, object]:
    locale = locale_for_user(user)
    admin_state = UserAdminState.objects.filter(telegram_user_id=user.tg_user_id).first()
    subscription = Subscription.objects.filter(user_id=user.tg_user_id).order_by("-created_at").first()
    profile = BillingProfile.objects.filter(user_id=user.tg_user_id).first()

    subscription_status = str(subscription.status if subscription else "")
    expires_at = subscription.expires_at if subscription else None
    grace_expires_at = subscription.grace_expires_at if subscription else None
    last_failure_reason = str(profile.last_failure_reason if profile else "")
    soft_grace_active = (
        subscription_status == Subscription.Status.EXPIRED
        and grace_expires_at is not None
        and _is_soft_grace_reason(last_failure_reason)
    )
    billing_access_mode = _billing_access_mode(
        subscription_status=subscription_status,
        expires_at=expires_at,
        grace_expires_at=grace_expires_at,
        soft_grace_active=soft_grace_active,
    )

    access_scope = str(admin_state.access_scope if admin_state else UserAdminState.AccessScope.PAYWALL)
    is_blocked = bool(admin_state.is_blocked or getattr(admin_state, "status", "") == "banned") if admin_state else False
    scope = _resolve_finance_scope(user, refresh=True)
    sponsored = not is_blocked and scope.is_family and scope.role != "owner" and _family_sponsor_has_access(scope.family_id)
    personal_full = billing_access_mode == "full" and (subscription_status in PERSONAL_FULL_STATUSES or soft_grace_active)
    # AccessScope is only a UI projection. Manual/lifetime grants live in Subscription.
    access_scope = "family_full" if sponsored else "personal_full" if personal_full else "debt_only" if access_scope == "debt_only" else "paywall"

    if is_blocked:
        mode = "blocked"
    elif sponsored or personal_full:
        mode = "active"
    elif billing_access_mode == "read_only":
        mode = "grace_read_only"
    else:
        mode = "blocked"

    billing_banner = None
    if mode == "grace_read_only":
        billing_banner = mt(
            "Доступ лише для перегляду. Онови оплату, щоб повернути повний доступ у боті.",
            "Read-only access is active. Update the payment to restore full access in the bot.",
            locale,
        )
    elif mode == "blocked":
        billing_banner = mt(
            "Доступ до кабінету тимчасово закритий. Потрібно відновити підписку.",
            "Dashboard access is temporarily closed. You need to restore the subscription.",
            locale,
        )

    has_card = bool(profile and profile.card_token)
    card_status = "bound" if has_card else "not_bound"
    card_status_label = mt("Картка прив'язана", "Card linked", locale) if has_card else mt(
        "Картка ще не прив'язана",
        "Card not linked yet",
        locale,
    )
    trial_days = int(subscription.trial_days or 0) if subscription else 0
    if trial_days <= 0:
        trial_days = int(getattr(settings, "MONO_RENEWAL_PERIOD_DAYS", 30) or 30)

    bind_amount_label = _money_minor_units_to_label(getattr(settings, "MONO_BIND_AMOUNT", 100), "UAH")
    monthly_price_label = _money_minor_units_to_label(getattr(settings, "MONO_RENEWAL_AMOUNT", 49900), "UAH")
    renewal_period_days = int(getattr(settings, "MONO_RENEWAL_PERIOD_DAYS", 30) or 30)
    pending_start_payload = str(admin_state.pending_start_payload if admin_state else "").strip()
    promo_code = pending_start_payload.removeprefix("promo_").strip().upper() if pending_start_payload.startswith("promo_") else ""
    promo_trial_days = 0
    if promo_code:
        promo_offer = PromoOffer.objects.filter(
            code__iexact=promo_code,
            is_active=True,
        ).filter(
            Q(starts_at__isnull=True) | Q(starts_at__lte=timezone.now()),
            Q(ends_at__isnull=True) | Q(ends_at__gte=timezone.now()),
        ).first()
        if promo_offer is not None:
            promo_code = str(promo_offer.code or "").strip().upper()
            promo_trial_days = int(promo_offer.trial_days or 0)
        else:
            promo_code = ""
    elif pending_start_payload == "course" or pending_start_payload.startswith("course_"):
        promo_trial_days = 90
    elif pending_start_payload == "treads":
        promo_trial_days = 30

    recommended_action = None
    entry_title = None
    entry_message = None
    if mode == "grace_read_only":
        recommended_action = "resume_subscription"
        entry_title = mt("Доступ лише для перегляду", "Read-only access", locale)
        entry_message = mt(
            "Онови оплату в боті, щоб знову відкрити повний доступ до кабінету.",
            "Update the payment in the bot to restore full dashboard access.",
            locale,
        )
    elif mode == "blocked":
        if has_card:
            recommended_action = "resume_subscription"
            entry_title = mt("Віднови доступ до кабінету", "Restore dashboard access", locale)
            entry_message = mt(
                "Картка вже прив'язана, але доступ до кабінету ще не активний. Повернись у бот і заверши активацію або віднови оплату.",
                "The card is already linked, but dashboard access is not active yet. Return to the bot and complete activation or restore payment.",
                locale,
            )
        else:
            recommended_action = "start_trial"
            entry_title = mt(
                f"Спробуй кабінет {trial_days} днів",
                f"Try the dashboard for {trial_days} days",
                locale,
            )
            entry_message = mt(
                f"Щоб відкрити кабінет, прив'яжи картку в боті. Зараз спишемо {bind_amount_label} для перевірки, а після пробного періоду буде {monthly_price_label} раз на {renewal_period_days} днів.",
                f"To open the dashboard, link a card in the bot. We'll charge {bind_amount_label} now for verification, then {monthly_price_label} every {renewal_period_days} days after the trial.",
                locale,
            )

    return {
        "locale": locale,
        "mode": mode,
        "access_scope": access_scope,
        "subscription_status": subscription_status,
        "billing_access_mode": billing_access_mode,
        "expires_at": expires_at.isoformat() if expires_at else None,
        "next_charge_at": subscription.next_charge_at.isoformat() if subscription and subscription.next_charge_at else None,
        "grace_expires_at": grace_expires_at.isoformat() if grace_expires_at else None,
        "has_card": has_card,
        "card_status": card_status,
        "card_status_label": card_status_label,
        "masked_pan": str(profile.masked_pan or "") if profile else "",
        "billing_profile_status": str(profile.status or "") if profile else "",
        "auto_renew_enabled": bool(profile.auto_renew_enabled) if profile else False,
        "last_charge_status": str(profile.last_charge_status or "") if profile else "",
        "last_failure_reason": last_failure_reason,
        "last_action_url": str(profile.last_action_url or "") if profile else "",
        "billing_banner": billing_banner,
        "trial_days": trial_days,
        "promo_code": promo_code,
        "promo_trial_days": promo_trial_days,
        "bind_amount_label": bind_amount_label,
        "monthly_price_label": monthly_price_label,
        "renewal_period_days": renewal_period_days,
        "recommended_action": recommended_action,
        "entry_title": entry_title,
        "entry_message": entry_message,
    }


def reminder_settings_payload(setting) -> dict[str, object]:
    raw_mode = str(getattr(setting, "mode", "") or "").strip().lower()
    raw_hour = getattr(setting, "reminder_hour", None)
    try:
        reminder_hour = int(raw_hour)
    except (TypeError, ValueError):
        reminder_hour = DEFAULT_REMINDER_HOUR
    return {
        "mode": raw_mode if raw_mode in SUPPORTED_REMINDER_MODES else DEFAULT_REMINDER_MODE,
        "reminder_hour": reminder_hour if reminder_hour in SUPPORTED_REMINDER_HOURS else DEFAULT_REMINDER_HOUR,
        "timezone": DEFAULT_REMINDER_TIMEZONE,
        "modes": sorted(SUPPORTED_REMINDER_MODES),
        "hours": sorted(SUPPORTED_REMINDER_HOURS),
    }


def profile_settings_payload(user: TelegramUser, *, reminder_setting=None) -> dict[str, object]:
    base_currency = _normalize_currency(str(user.base_currency or "UAH"))
    currencies = list(DEFAULT_BASE_CURRENCIES)
    if base_currency not in currencies:
        currencies.append(base_currency)
    return {
        "user": {
            "tg_user_id": int(user.tg_user_id),
            "first_name": str(user.first_name or ""),
            "base_currency": base_currency,
            "lang": locale_for_user(user),
        },
        "base_currencies": currencies,
        "reminder": reminder_settings_payload(reminder_setting),
        "savings": saving_prompt_settings_payload(user),
        "access": resolve_access(user),
        "support_url": str(getattr(settings, "SUPPORT_CONTACT_URL", "") or "").strip(),
    }


def saving_prompt_settings_payload(user: TelegramUser) -> dict[str, object]:
    setting = SavingPromptSetting.objects.filter(user_id=user.tg_user_id).first()
    target_accounts = (
        Account.objects.filter(
            tg_user_id=user.tg_user_id,
            is_active=True,
            account_type__in=GOAL_TYPES,
        )
        .order_by("label", "id")
    )
    default_target_id = getattr(setting, "default_target_account_id", None)
    return {
        "ask_after_income": bool(getattr(setting, "ask_after_income", True)),
        "default_percent": f"{_quantize(getattr(setting, 'default_percent', 10)):.2f}",
        "ask_only_for_salary": bool(getattr(setting, "ask_only_for_salary", False)),
        "min_income_amount": (
            f"{_quantize(setting.min_income_amount):.2f}"
            if setting is not None and setting.min_income_amount is not None
            else ""
        ),
        "default_target_account_id": int(default_target_id) if default_target_id else None,
        "target_accounts": [
            {
                "id": int(account.id),
                "label": str(account.label or ""),
                "currency": _normalize_currency(account.currency),
            }
            for account in target_accounts
        ],
    }


def update_profile_settings(user: TelegramUser, payload: dict[str, object]) -> dict[str, object]:
    locale = normalize_locale(str(payload.get("lang") or ""), default=locale_for_user(user))
    base_currency = _parse_currency_code(payload.get("base_currency"), default=_base_currency(user))
    user.lang = locale
    user.base_currency = base_currency
    user.save(update_fields=["lang", "base_currency"])
    return {
        "lang": locale,
        "base_currency": base_currency,
    }


def _parse_setting_boolean(value: object, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise MiniAppTransactionError("invalid_saving_settings", f"{field} must be true or false.")


def update_saving_prompt_settings(user: TelegramUser, payload: dict[str, object]) -> None:
    percent = _parse_signed_transaction_amount(
        payload.get("default_percent"),
        code="invalid_saving_settings",
        message="Saving percentage must be a number from 0 to 100.",
    )
    if percent <= ZERO or percent > ONE_HUNDRED:
        raise MiniAppTransactionError("invalid_saving_settings", "Saving percentage must be a number from 0 to 100.")
    min_income_raw = str(payload.get("min_income_amount") or "").strip()
    min_income = None
    if min_income_raw:
        min_income = _parse_signed_transaction_amount(
            min_income_raw,
            code="invalid_saving_settings",
            message="Minimum income must be a non-negative number.",
        )
        if min_income < ZERO:
            raise MiniAppTransactionError("invalid_saving_settings", "Minimum income must be a non-negative number.")
        if min_income == ZERO:
            min_income = None

    raw_target_id = str(payload.get("default_target_account_id") or "").strip()
    target_id = None
    if raw_target_id:
        target_id = _parse_positive_int(
            raw_target_id,
            code="invalid_saving_settings",
            message="Choose a savings target account.",
        )
        exists = Account.objects.filter(
            id=target_id,
            tg_user_id=user.tg_user_id,
            is_active=True,
            account_type__in=GOAL_TYPES,
        ).exists()
        if not exists:
            raise MiniAppTransactionError("invalid_saving_settings", "Choose an active savings target account.")

    SavingPromptSetting.objects.update_or_create(
        user_id=user.tg_user_id,
        defaults={
            "enabled": True,
            "ask_after_income": _parse_setting_boolean(payload.get("ask_after_income"), field="ask_after_income"),
            "default_percent": percent,
            "ask_only_for_salary": _parse_setting_boolean(payload.get("ask_only_for_salary"), field="ask_only_for_salary"),
            "min_income_amount": min_income,
            "default_target_account_id": target_id,
        },
    )


def resolve_period(params, *, locale: str = "uk") -> PeriodSelection:
    today = timezone.localdate()
    preset = str(params.get("preset") or "this_month").strip().lower()

    if preset == "today":
        return PeriodSelection(preset=preset, date_from=today, date_to=today, label=mt("Сьогодні", "Today", locale))
    if preset == "yesterday":
        yesterday = today - timedelta(days=1)
        return PeriodSelection(preset=preset, date_from=yesterday, date_to=yesterday, label=mt("Вчора", "Yesterday", locale))
    if preset in {"last_7", "7_days", "7d"}:
        return PeriodSelection(preset="last_7", date_from=today - timedelta(days=6), date_to=today, label=mt("7 днів", "7 days", locale))
    if preset in {"last_30", "30_days", "30d"}:
        return PeriodSelection(preset="last_30", date_from=today - timedelta(days=29), date_to=today, label=mt("30 днів", "30 days", locale))
    if preset == "custom":
        raw_from = str(params.get("date_from") or "")
        raw_to = str(params.get("date_to") or "")
        try:
            date_from = date.fromisoformat(raw_from)
            date_to = date.fromisoformat(raw_to)
        except ValueError as exc:
            raise ValueError("invalid_period") from exc
        if date_from > date_to or (date_to - date_from).days > 365:
            raise ValueError("invalid_period")
        return PeriodSelection(
            preset="custom",
            date_from=date_from,
            date_to=date_to,
            label=f"{date_from.strftime('%d.%m')} - {date_to.strftime('%d.%m')}",
        )

    month_start = today.replace(day=1)
    return PeriodSelection(preset="this_month", date_from=month_start, date_to=today, label=mt("Цей місяць", "This month", locale))


def comparable_previous_period(period: PeriodSelection) -> PeriodSelection:
    span_days = (period.date_to - period.date_from).days + 1
    previous_end = period.date_from - timedelta(days=1)
    previous_start = previous_end - timedelta(days=span_days - 1)
    return PeriodSelection(
        preset="previous_comparable",
        date_from=previous_start,
        date_to=previous_end,
        label="Попередній період",
    )


def _base_currency(user: TelegramUser) -> str:
    return _normalize_currency(str(user.base_currency or "UAH"))


def _resolve_finance_scope(user: TelegramUser, *, refresh: bool = False) -> MiniAppFinanceScope:
    cached_scope = getattr(user, "_miniapp_finance_scope", None)
    if cached_scope is not None and not refresh:
        return cached_scope

    existing_tables = set(connection.introspection.table_names())
    if not {"families", "family_members"}.issubset(existing_tables):
        scope = MiniAppFinanceScope(type="personal", user_id=int(user.tg_user_id))
        setattr(user, "_miniapp_finance_scope", scope)
        return scope

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT fm.family_id, fm.role
            FROM family_members fm
            JOIN families f ON f.id = fm.family_id
            WHERE fm.user_id = %s
              AND fm.status = 'active'
              AND f.status = 'active'
            ORDER BY CASE WHEN fm.role = 'owner' THEN 0 ELSE 1 END, fm.joined_at ASC, fm.id ASC
            LIMIT 1
            """,
            [int(user.tg_user_id)],
        )
        row = cursor.fetchone()

    if row is None:
        scope = MiniAppFinanceScope(type="personal", user_id=int(user.tg_user_id))
    else:
        scope = MiniAppFinanceScope(
            type="family",
            user_id=int(user.tg_user_id),
            family_id=int(row[0]),
            role=str(row[1] or "member"),
        )

    setattr(user, "_miniapp_finance_scope", scope)
    return scope


def _lock_finance_scope(user: TelegramUser) -> MiniAppFinanceScope:
    # Actor row protects absent membership; family row serializes shared labels.
    TelegramUser.objects.select_for_update().get(tg_user_id=user.tg_user_id)
    scope = _resolve_finance_scope(user, refresh=True)
    if scope.is_family:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM families WHERE id=%s AND status='active' FOR UPDATE", [scope.family_id])
            if cursor.fetchone() is None:
                raise MiniAppTransactionError("scope_changed", "The finance scope changed. Create a new preview.", status=409)
    return scope


def _scoped_accounts_qs(user: TelegramUser):
    scope = _resolve_finance_scope(user)
    if scope.is_family:
        return Account.objects.filter(family_id=scope.family_id)
    return Account.objects.filter(tg_user_id=user.tg_user_id, family_id__isnull=True)


def _scoped_transactions_qs(user: TelegramUser):
    scope = _resolve_finance_scope(user)
    if scope.is_family:
        return Transaction.objects.filter(family_id=scope.family_id)
    return Transaction.objects.filter(tg_user_id=user.tg_user_id, family_id__isnull=True)


def _scoped_debts_qs(user: TelegramUser):
    scope = _resolve_finance_scope(user)
    if scope.is_family:
        return Debt.objects.filter(family_id=scope.family_id)
    return Debt.objects.filter(tg_user_id=user.tg_user_id, family_id__isnull=True)


def _scoped_categories_qs(user: TelegramUser):
    scope = _resolve_finance_scope(user)
    if scope.is_family:
        return Category.objects.filter(family_id=scope.family_id)
    personal_scope = Q(family_id__isnull=True) & (Q(user_id=user.tg_user_id) | Q(tg_user_id=user.tg_user_id))
    system_scope = Q(family_id__isnull=True, user_id__isnull=True, tg_user_id__isnull=True, is_system=True)
    return Category.objects.filter(personal_scope | system_scope)


def _normalize_transaction_kind(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in SUPPORTED_NORMAL_TRANSACTION_KINDS:
        raise MiniAppTransactionError("invalid_payload", "Transaction type must be expense or income.")
    return normalized


def _bounded_decimal(value: object, *, quantum: str = "0.01", maximum: str = "9999999999999999.99", code: str = "invalid_payload", message: str = "Amount is outside the supported range.") -> Decimal:
    raw = str(value if value is not None else "").strip().replace(" ", "").replace(",", ".")
    try:
        if len(raw) > 128:
            raise ValueError("numeric input too long")
        amount = Decimal(raw)
        if not amount.is_finite() or amount.copy_abs() > Decimal(maximum):
            raise ValueError("numeric input outside range")
        return amount.quantize(Decimal(quantum), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError) as exc:
        raise MiniAppTransactionError(code, message) from exc


def _parse_transaction_amount(value: object) -> Decimal:
    amount = _bounded_decimal(value)
    if amount <= ZERO:
        raise MiniAppTransactionError("invalid_payload", "Amount must be greater than zero.")
    return amount


def _parse_positive_int(value: object, *, code: str, message: str) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError) as exc:
        raise MiniAppTransactionError(code, message) from exc
    if parsed <= 0:
        raise MiniAppTransactionError(code, message)
    return parsed


def _parse_transaction_date(value: object) -> date:
    raw = str(value or "").strip()
    if not raw:
        return timezone.localdate()
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise MiniAppTransactionError("invalid_payload", "Transaction date must be a valid ISO date.") from exc


def _normalize_account_type(value: object) -> str:
    key = str(value or "").strip().lower()
    key = LEGACY_ACCOUNT_TYPE_ALIASES.get(key, key)
    return key if key in SUPPORTED_ACCOUNT_TYPES else "other"


def _default_non_negative_account_type(value: object) -> str:
    normalized = _normalize_account_type(value)
    if normalized == CREDIT_ACCOUNT_TYPE:
        return "main"
    return normalized


def _goal_account_type(account: Account) -> str:
    current_raw = str(account.account_type or "").strip()
    if not current_raw:
        return _normalize_account_type(account.non_negative_account_type)
    current = _normalize_account_type(current_raw)
    if current == CREDIT_ACCOUNT_TYPE:
        return _normalize_account_type(account.non_negative_account_type or "main")
    return current


def _resolve_runtime_account_type(
    *,
    balance: Decimal,
    non_negative_account_type: str | None,
    current_account_type: str | None,
) -> str:
    if _quantize(balance) < ZERO:
        return CREDIT_ACCOUNT_TYPE
    return _default_non_negative_account_type(non_negative_account_type or current_account_type or "main")


def _category_kind(category: Category) -> str:
    return str(category.type or category.kind or "").strip().lower()


def _category_payload(category: Category) -> dict[str, object]:
    kind = _category_kind(category)
    return {
        "id": int(category.id),
        "name": str(category.name or ""),
        "kind": kind,
        "type": kind,
    }


def _account_payload(account: Account, *, locale: str = "uk") -> dict[str, object]:
    return {
        "id": int(account.id),
        "label": str(account.label or ""),
        "account_type": str(account.account_type or ""),
        "type_label": _account_type_label(str(account.account_type or ""), locale=locale),
        "currency": _normalize_currency(account.currency),
        "balance": money_payload(account.balance, account.currency),
        "non_negative_account_type": _normalize_account_type(account.non_negative_account_type or account.account_type),
        "credit_limit": money_payload(account.credit_limit, account.currency) if account.credit_limit is not None else None,
        "goal_name": str(account.goal_name or "") or None,
        "goal_amount": money_payload(account.goal_amount, account.currency) if account.goal_amount is not None else None,
        "goal_date": account.goal_date.isoformat() if account.goal_date else None,
    }


def _active_scoped_account(user: TelegramUser, account_id: int, *, for_update: bool = False) -> Account | None:
    qs = _scoped_accounts_qs(user).filter(is_active=True, id=account_id)
    if for_update:
        qs = qs.select_for_update()
    return qs.first()


def _active_scoped_category(user: TelegramUser, category_id: int, kind: str) -> Category:
    qs = _scoped_categories_qs(user).filter(is_active=True, deleted_at__isnull=True, id=category_id)
    category = qs.first()
    if category is None:
        raise MiniAppTransactionError("category_missing", "Category is not available.", status=404)
    if _category_kind(category) != kind:
        raise MiniAppTransactionError("category_kind_mismatch", "Category does not match transaction type.")
    return category


def build_transaction_options(user: TelegramUser, *, kind: str | None = None) -> dict[str, object]:
    locale = locale_for_user(user)
    normalized_kind = _normalize_transaction_kind(kind) if kind else None
    accounts = [
        _account_payload(account, locale=locale)
        for account in _scoped_accounts_qs(user).filter(is_active=True).order_by("label", "id")[:100]
    ]
    categories_qs = _scoped_categories_qs(user).filter(is_active=True, deleted_at__isnull=True)
    if normalized_kind:
        categories_qs = categories_qs.filter(Q(type__iexact=normalized_kind) | Q(kind__iexact=normalized_kind))

    categories_by_kind: dict[str, list[dict[str, object]]] = {"expense": [], "income": []}
    for category in categories_qs.order_by("-is_system", "sort_order", "name", "id")[:200]:
        category_kind = _category_kind(category)
        if category_kind in categories_by_kind:
            categories_by_kind[category_kind].append(_category_payload(category))

    return {
        "accounts": accounts,
        "categories": categories_by_kind,
        "default_kind": normalized_kind or "expense",
    }


def _ai_text_account_matches_hint(account: Account, hint: str) -> bool:
    normalized_hint = str(hint or "").strip().lower()
    label = str(account.label or "").strip().lower()
    account_type = _normalize_account_type(account.account_type)
    hint_tokens = {
        "monobank": ("моно", "mono", "monobank"),
        "privatbank": ("приват", "privat", "privatbank"),
        "raiffeisen": ("райф", "raif", "raiffeisen"),
        "cash": ("готів", "налич", "cash", "кеш"),
    }.get(normalized_hint, (normalized_hint,))
    return (normalized_hint == "cash" and account_type == "cash") or any(token and token in label for token in hint_tokens)


def _pick_ai_text_account(
    user: TelegramUser,
    *,
    account_hint: str | None,
    currency: str,
    currency_explicit: bool,
) -> Account | None:
    accounts = list(_scoped_accounts_qs(user).filter(is_active=True).order_by("label", "id")[:100])
    if currency_explicit:
        accounts = [account for account in accounts if _normalize_currency(account.currency) == currency]
    if account_hint:
        matched = [account for account in accounts if _ai_text_account_matches_hint(account, account_hint)]
        return matched[0] if len(matched) == 1 else None
    return accounts[0] if len(accounts) == 1 else None


def _pick_ai_text_category(user: TelegramUser, *, kind: str, slug: str | None) -> Category | None:
    normalized_slug = str(slug or "").strip().lower()
    if not normalized_slug:
        return None
    candidates = _scoped_categories_qs(user).filter(
        is_active=True,
        deleted_at__isnull=True,
        slug__iexact=normalized_slug,
    ).order_by("id")
    for category in candidates:
        if _category_kind(category) == kind:
            return category
    return None


def guard_ai_text_intent(parsed: dict[str, object], source_text: str) -> dict[str, object]:
    """Prevent an explicit account-to-account command from hydrating a normal transaction."""

    guarded = dict(parsed)
    if AI_TRANSFER_COMMAND_PATTERN.search(source_text or "") and AI_TRANSFER_ROUTE_PATTERN.search(source_text or ""):
        guarded["intent"] = "transfer"
        guarded["is_candidate_tx"] = True
        guarded["category_slug"] = None
    return guarded


def build_ai_text_suggestion(user: TelegramUser, parsed: dict[str, object]) -> dict[str, object]:
    locale = locale_for_user(user)
    kind = str(parsed.get("intent") or "").strip().lower()
    if kind in {"transfer", "debt"}:
        return {
            "mode": "redirect",
            "intent": kind,
            "screen": "money",
            "money_mode": "debts" if kind == "debt" else "accounts",
        }
    if kind not in SUPPORTED_NORMAL_TRANSACTION_KINDS or not bool(parsed.get("is_candidate_tx")):
        return {"mode": "unrecognized"}

    base_currency = _base_currency(user)
    currency = _normalize_currency(parsed.get("currency"), default=base_currency)
    currency_explicit = bool(parsed.get("currency_explicit"))
    amount: Decimal | None = None
    raw_amount = parsed.get("amount")
    if raw_amount is not None:
        try:
            candidate_amount = _quantize(Decimal(str(raw_amount)))
        except (InvalidOperation, ValueError):
            candidate_amount = ZERO
        if candidate_amount > ZERO:
            amount = candidate_amount

    transaction_date = timezone.localdate()
    raw_date = str(parsed.get("transaction_date") or "").strip()
    if raw_date:
        try:
            transaction_date = date.fromisoformat(raw_date)
        except ValueError:
            pass

    account = _pick_ai_text_account(
        user,
        account_hint=str(parsed.get("account_hint") or "") or None,
        currency=currency,
        currency_explicit=currency_explicit,
    )
    category = _pick_ai_text_category(
        user,
        kind=kind,
        slug=str(parsed.get("category_slug") or "") or None,
    )
    missing_fields: list[str] = []
    if amount is None:
        missing_fields.append("amount")
    if account is None:
        missing_fields.append("account_id")
    if category is None:
        missing_fields.append("category_id")

    return {
        "mode": "transaction",
        "kind": kind,
        "amount": f"{amount:.2f}" if amount is not None else None,
        "currency": currency,
        "currency_explicit": currency_explicit,
        "account_id": int(account.id) if account is not None else None,
        "category_id": int(category.id) if category is not None else None,
        "transaction_date": transaction_date.isoformat(),
        "comment": str(parsed.get("comment") or "").strip()[:500],
        "missing_fields": missing_fields,
        "confidence": parsed.get("confidence") if isinstance(parsed.get("confidence"), dict) else {},
        "message": mt("Перевірте заповнені поля перед підтвердженням.", "Review the populated fields before confirming.", locale),
    }


def detect_ai_image_mime(image_bytes: bytes) -> str | None:
    if image_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if image_bytes.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(image_bytes) >= 12 and image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    return detect_heif_mime(image_bytes)


def detect_ai_voice_mime(audio_bytes: bytes) -> str | None:
    if audio_bytes.startswith(b"OggS"):
        return "audio/ogg"
    if audio_bytes.startswith(b"\x1aE\xdf\xa3") and b"webm" in audio_bytes[:4096].lower():
        return "audio/webm"
    if len(audio_bytes) >= 12 and audio_bytes[4:8] == b"ftyp":
        return "audio/mp4"
    return None


def build_ai_image_catalog(user: TelegramUser) -> dict[str, object]:
    accounts = [
        {"id": int(account.id), "label": str(account.label or "")[:120]}
        for account in _scoped_accounts_qs(user).filter(is_active=True).order_by("label", "id")[:100]
    ]
    categories: dict[str, list[dict[str, object]]] = {"expense": [], "income": []}
    for category in _scoped_categories_qs(user).filter(is_active=True, deleted_at__isnull=True).order_by("-is_system", "sort_order", "name", "id")[:200]:
        category_kind = _category_kind(category)
        if category_kind in categories:
            categories[category_kind].append(
                {
                    "id": int(category.id),
                    "name": str(category.name or "")[:120],
                    "aliases": [str(alias)[:120] for alias in list(category.aliases or [])[:40] if str(alias).strip()],
                }
            )
    return {
        "accounts": accounts,
        "expense_categories": categories["expense"],
        "income_categories": categories["income"],
    }


def _ai_image_token(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().casefold())


def _pick_ai_image_account(user: TelegramUser, parsed: dict[str, object], *, currency: str, currency_explicit: bool) -> Account | None:
    accounts = list(_scoped_accounts_qs(user).filter(is_active=True).order_by("label", "id")[:100])
    if currency_explicit:
        accounts = [account for account in accounts if _normalize_currency(account.currency) == currency]
    hints = [
        str(parsed.get(key) or "").strip()
        for key in ("account_hint", "bank_name", "card_last4")
        if str(parsed.get(key) or "").strip()
    ]
    if hints:
        matched = [account for account in accounts if any(_ai_text_account_matches_hint(account, hint) for hint in hints)]
        return matched[0] if len(matched) == 1 else None
    return accounts[0] if len(accounts) == 1 else None


def _pick_ai_image_category(user: TelegramUser, *, kind: str, category_hint: object) -> Category | None:
    hint = _ai_image_token(category_hint)
    if not hint:
        return None
    matches: list[Category] = []
    for category in _scoped_categories_qs(user).filter(is_active=True, deleted_at__isnull=True).order_by("id")[:200]:
        if _category_kind(category) != kind:
            continue
        values = [category.slug, category.name, *(category.aliases or [])]
        if any(_ai_image_token(value) == hint for value in values):
            matches.append(category)
    return matches[0] if len(matches) == 1 else None


def build_ai_image_suggestion(user: TelegramUser, parsed: dict[str, object]) -> dict[str, object]:
    mode = str(parsed.get("mode") or "").strip().lower()
    if mode == "statement_expenses":
        return {
            "mode": "batch",
            "item_count": len(parsed.get("statement_items") or []),
            "skipped_items_count": int(parsed.get("skipped_items_count") or 0),
        }
    kind = str(parsed.get("type") or "").strip().lower()
    if mode != "single_tx" or kind not in SUPPORTED_NORMAL_TRANSACTION_KINDS:
        return {"mode": "unrecognized"}

    locale = locale_for_user(user)
    base_currency = _base_currency(user)
    currency = _normalize_currency(parsed.get("currency"), default=base_currency)
    currency_explicit = bool(parsed.get("currency_explicit"))
    amount: Decimal | None = None
    if parsed.get("amount") is not None:
        try:
            candidate_amount = _quantize(Decimal(str(parsed.get("amount"))))
        except (InvalidOperation, ValueError):
            candidate_amount = ZERO
        if candidate_amount > ZERO:
            amount = candidate_amount

    transaction_date = timezone.localdate()
    raw_date = str(parsed.get("transaction_date") or "").strip()
    if raw_date:
        try:
            transaction_date = date.fromisoformat(raw_date)
        except ValueError:
            pass

    account = _pick_ai_image_account(user, parsed, currency=currency, currency_explicit=currency_explicit)
    category = _pick_ai_image_category(user, kind=kind, category_hint=parsed.get("category_hint"))
    missing_fields: list[str] = []
    if amount is None:
        missing_fields.append("amount")
    if account is None:
        missing_fields.append("account_id")
    if category is None:
        missing_fields.append("category_id")

    return {
        "mode": "transaction",
        "kind": kind,
        "amount": f"{amount:.2f}" if amount is not None else None,
        "currency": currency,
        "currency_explicit": currency_explicit,
        "account_id": int(account.id) if account is not None else None,
        "category_id": int(category.id) if category is not None else None,
        "transaction_date": transaction_date.isoformat(),
        "comment": str(parsed.get("comment") or "").strip()[:500],
        "missing_fields": missing_fields,
        "confidence": float(parsed.get("confidence") or 0),
        "message": mt("Перевірте заповнені поля перед підтвердженням.", "Review the populated fields before confirming.", locale),
    }


def build_ai_image_batch(user: TelegramUser, parsed: dict[str, object], *, batch_id: str) -> dict[str, object]:
    """Build a review-only statement batch from normalized Vision output."""
    raw_items = parsed.get("statement_items")
    if not isinstance(raw_items, list):
        raw_items = []

    items: list[dict[str, object]] = []
    invalid_items_count = 0
    for raw_item in raw_items[:AI_IMAGE_BATCH_MAX_ITEMS]:
        if not isinstance(raw_item, dict):
            invalid_items_count += 1
            continue
        suggestion = build_ai_image_suggestion(
            user,
            {
                "mode": "single_tx",
                # Vision statement mode is expense-only. Do not let a malformed row
                # change the batch into an income capture path.
                "type": "expense",
                "amount": raw_item.get("amount"),
                "currency": raw_item.get("currency") or parsed.get("dominant_currency") or parsed.get("currency"),
                "currency_explicit": bool(parsed.get("currency_explicit")),
                "account_hint": parsed.get("account_hint"),
                "bank_name": parsed.get("bank_name"),
                "card_last4": parsed.get("card_last4"),
                "category_hint": raw_item.get("category_hint"),
                "transaction_date": raw_item.get("transaction_date"),
                "comment": raw_item.get("comment"),
                "confidence": raw_item.get("confidence"),
            },
        )
        if suggestion.get("mode") != "transaction":
            invalid_items_count += 1
            continue
        items.append(
            {
                "id": str(len(items) + 1),
                "status": "pending",
                "suggestion": suggestion,
            }
        )

    skipped_items_count = max(0, int(parsed.get("skipped_items_count") or 0))
    skipped_items_count += invalid_items_count + max(0, len(raw_items) - AI_IMAGE_BATCH_MAX_ITEMS)
    return {
        "mode": "batch",
        "batch_id": batch_id,
        "currency": _normalize_currency(parsed.get("dominant_currency") or parsed.get("currency"), default=_base_currency(user)),
        "currency_explicit": bool(parsed.get("currency_explicit")),
        "items": items,
        "item_count": len(items),
        "skipped_items_count": skipped_items_count,
    }


def build_category_catalog(user: TelegramUser) -> dict[str, object]:
    categories_by_kind: dict[str, list[dict[str, object]]] = {"expense": [], "income": []}
    categories_qs = _scoped_categories_qs(user).filter(is_active=True, deleted_at__isnull=True)
    for category in categories_qs.order_by("-is_system", "sort_order", "name", "id")[:200]:
        category_kind = _category_kind(category)
        if category_kind in categories_by_kind:
            categories_by_kind[category_kind].append(_category_payload(category))
    return {
        "mode": "view_only",
        "categories": categories_by_kind,
    }


def build_transaction_draft(user: TelegramUser, payload: dict[str, object], *, draft_id: str) -> dict[str, object]:
    locale = locale_for_user(user)
    kind = _normalize_transaction_kind(payload.get("kind"))
    amount = _parse_transaction_amount(payload.get("amount"))
    account_id = _parse_positive_int(
        payload.get("account_id"),
        code="account_missing",
        message="Account is required.",
    )
    category_id = _parse_positive_int(
        payload.get("category_id"),
        code="category_missing",
        message="Category is required.",
    )
    account = _active_scoped_account(user, account_id)
    if account is None:
        raise MiniAppTransactionError("account_missing", "Account is not available.", status=404)
    category = _active_scoped_category(user, category_id, kind)
    account_currency = _normalize_currency(account.currency)
    currency = _normalize_currency(payload.get("currency") or account_currency)
    if currency != account_currency:
        raise MiniAppTransactionError(
            "currency_account_mismatch",
            "Transaction currency must match selected account currency.",
            extra={"account": _account_payload(account, locale=locale)},
        )
    tx_date = _parse_transaction_date(payload.get("transaction_date") or payload.get("date"))
    comment = str(payload.get("comment") or "").strip()[:500]
    amount_payload = money_payload(amount, currency)
    return {
        "draft_id": draft_id,
        "kind": kind,
        "transaction_date": tx_date.isoformat(),
        "amount_value": f"{amount:.2f}",
        "amount": amount_payload,
        "currency": currency,
        "account": _account_payload(account, locale=locale),
        "account_id": int(account.id),
        "category": _category_payload(category),
        "category_id": int(category.id),
        "comment": comment,
        "source": MINIAPP_TRANSACTION_SOURCE,
        "confirmation": {
            "title": mt("Підтвердіть операцію", "Confirm transaction", locale),
            "summary": [
                amount_payload["display"],
                str(account.label or ""),
                str(category.name or ""),
                tx_date.isoformat(),
            ],
        },
    }


def build_billing_expense_options(user: TelegramUser) -> dict[str, object]:
    pending = (
        AiTransactionDraft.objects.filter(
            tg_user_id=user.tg_user_id,
            source=BILLING_EXPENSE_DRAFT_SOURCE,
            status="pending",
        )
        .order_by("-updated_at", "-id")
        .first()
    )
    if pending is None:
        return {"draft": None, "options": None}

    category = (
        _scoped_categories_qs(user)
        .filter(type="expense", slug="subscriptions_services", is_active=True, deleted_at__isnull=True)
        .order_by("id")
        .first()
    )
    return {
        "draft": {
            "id": int(pending.id),
            "transaction_date": pending.transaction_date.isoformat() if pending.transaction_date else timezone.localdate().isoformat(),
            "amount": money_payload(pending.amount, pending.currency),
            "amount_value": f"{_quantize(pending.amount):.2f}",
            "currency": _normalize_currency(pending.currency, default=_base_currency(user)),
            "comment": str(pending.comment or ""),
            "category_id": int(category.id) if category is not None else None,
        },
        "options": build_transaction_options(user, kind="expense"),
    }


def commit_transaction_draft(user: TelegramUser, draft: dict[str, object]) -> dict[str, object]:
    locale = locale_for_user(user)
    kind = _normalize_transaction_kind(draft.get("kind"))
    amount = _parse_transaction_amount(draft.get("amount_value"))
    account_id = _parse_positive_int(
        draft.get("account_id") or (draft.get("account") or {}).get("id"),
        code="account_missing",
        message="Account is required.",
    )
    category_id = _parse_positive_int(
        draft.get("category_id") or (draft.get("category") or {}).get("id"),
        code="category_missing",
        message="Category is required.",
    )
    currency = _normalize_currency(draft.get("currency"))
    tx_date = _parse_transaction_date(draft.get("transaction_date"))
    comment = str(draft.get("comment") or "").strip()[:500] or None
    source = str(draft.get("source") or MINIAPP_TRANSACTION_SOURCE).strip()
    if source not in {MINIAPP_TRANSACTION_SOURCE, MINIAPP_BILLING_EXPENSE_SOURCE}:
        raise MiniAppTransactionError("invalid_transaction_source", "Unsupported transaction source.")
    scope = _resolve_finance_scope(user)

    with db_transaction.atomic():
        account = _active_scoped_account(user, account_id, for_update=True)
        if account is None:
            raise MiniAppTransactionError("account_missing", "Account is not available.", status=404)
        category = _active_scoped_category(user, category_id, kind)
        account_currency = _normalize_currency(account.currency)
        if currency != account_currency:
            raise MiniAppTransactionError(
                "currency_account_mismatch",
                "Transaction currency must match selected account currency.",
                extra={"account": _account_payload(account, locale=locale)},
            )

        current_balance = _quantize(account.balance)
        balance_delta = amount if kind == "income" else -amount
        new_balance = _quantize(current_balance + balance_delta)
        current_account_type = _normalize_account_type(account.account_type)
        non_negative_account_type = _default_non_negative_account_type(
            account.non_negative_account_type or current_account_type or "main"
        )
        credit_limit = _quantize(account.credit_limit) if account.credit_limit is not None else None

        if kind == "expense" and new_balance < ZERO and credit_limit is not None and abs(new_balance) > credit_limit:
            raise MiniAppTransactionError(
                "credit_limit_exceeded",
                "This expense exceeds the account credit limit.",
                extra={
                    "previous_balance": money_payload(current_balance, account_currency),
                    "projected_balance": money_payload(new_balance, account_currency),
                    "credit_limit": money_payload(credit_limit, account_currency),
                },
            )

        new_account_type = _resolve_runtime_account_type(
            balance=new_balance,
            non_negative_account_type=non_negative_account_type,
            current_account_type=current_account_type,
        )
        tx = Transaction.objects.create(
            tg_user=user,
            created_by_user_id=user.tg_user_id,
            family_id=scope.family_id if scope.is_family else None,
            date=tx_date,
            type=kind,
            amount=amount,
            currency=account_currency,
            comment=comment,
            source=source,
            account=account,
            category=category,
            category_name_snapshot=category.name,
            flow_kind="normal",
            created_at=timezone.now(),
        )
        enqueue_transaction_created(
            actor_user_id=user.tg_user_id,
            space_id=scope.family_id if scope.is_family else None,
            transaction_id=tx.id,
            accepted_at=tx.created_at,
            source=source,
            transaction_type=kind,
            amount=amount,
            flow_kind="normal",
        )
        account.balance = new_balance
        account.account_type = new_account_type
        account.non_negative_account_type = non_negative_account_type
        account.save(update_fields=["balance", "account_type", "non_negative_account_type", "updated_at"])

    result_status = "completed"
    if current_account_type != CREDIT_ACCOUNT_TYPE and new_account_type == CREDIT_ACCOUNT_TYPE:
        result_status = "account_switched_to_credit"
    elif current_account_type == CREDIT_ACCOUNT_TYPE and new_account_type != CREDIT_ACCOUNT_TYPE:
        result_status = "account_restored_from_credit"

    return {
        "status": result_status,
        "transaction": {
            "id": int(tx.id),
            "date": tx_date.isoformat(),
            "kind": kind,
            "amount": money_payload(amount, account_currency),
            "currency": account_currency,
            "category": _category_payload(category),
            "comment": comment,
            "source": source,
        },
        "account": _account_payload(account, locale=locale),
        "previous_balance": money_payload(current_balance, account_currency),
        "new_balance": money_payload(new_balance, account_currency),
        "previous_account_type": current_account_type,
        "new_account_type": new_account_type,
        "dashboard_refresh": True,
    }


def _parse_signed_transaction_amount(
    value: object,
    *,
    code: str = "invalid_payload",
    message: str = "Amount must be a valid number.",
    default: Decimal | None = None,
) -> Decimal:
    raw = str(value or "").strip().replace(" ", "").replace(",", ".")
    if not raw and default is not None:
        return default
    return _bounded_decimal(raw, code=code, message=message)


def _parse_currency_code(value: object, *, default: str | None = None) -> str:
    normalized = str(value or default or "").strip().upper()
    if not CURRENCY_CODE_PATTERN.fullmatch(normalized):
        raise MiniAppTransactionError("invalid_currency", "Currency must contain 3 to 10 uppercase Latin letters.")
    return normalized


def _parse_optional_rate(value: object) -> Decimal | None:
    raw = str(value or "").strip().replace(" ", "").replace(",", ".")
    if not raw:
        return None
    # Rates are persisted in NUMERIC(18,6); the public precision remains 4 dp.
    rate = _bounded_decimal(raw, quantum="0.0001", maximum="999999999999.9999", code="invalid_fx_rate", message="Exchange rate must be a positive number within the supported range.")
    if rate <= ZERO:
        raise MiniAppTransactionError("invalid_fx_rate", "Exchange rate must be a positive number.")
    return rate


def _parse_optional_credit_limit(value: object) -> Decimal | None:
    raw = str(value or "").strip().replace(" ", "").replace(",", ".")
    if not raw:
        return None
    return _bounded_decimal(
        raw,
        quantum="0.01",
        maximum="9999999999999999.99",
        code="invalid_credit_limit",
        message="Credit limit is outside the supported range.",
    )


def _parse_account_label(value: object) -> str:
    label = str(value or "").strip()
    if not label:
        raise MiniAppTransactionError("account_label_required", "Account name is required.")
    if len(label) > 80:
        raise MiniAppTransactionError("account_label_too_long", "Account name is too long.")
    return label


def _parse_account_action(value: object) -> str:
    action = str(value or "").strip().lower()
    if action not in ACCOUNT_ACTIONS:
        raise MiniAppTransactionError("invalid_account_action", "Account action is not supported.")
    return action


def _account_type_options(locale: str) -> list[dict[str, str]]:
    values = ("main", "cash", "savings", "deposit", "investment", "credit", "other")
    return [
        {"id": value, "label": _account_type_label(value, locale=locale)}
        for value in values
    ]


def build_account_options(user: TelegramUser) -> dict[str, object]:
    locale = locale_for_user(user)
    base_currency = _base_currency(user)
    scope = _resolve_finance_scope(user)
    currencies = []
    for currency in (base_currency, "UAH", "USD", "EUR", "TRY", "USDT"):
        if currency not in currencies:
            currencies.append(currency)
    return {
        "scope": {
            "type": scope.type,
            "family_id": scope.family_id,
            "role": scope.role,
        },
        "accounts": [
            _account_payload(account, locale=locale)
            for account in _scoped_accounts_qs(user).filter(is_active=True).order_by("label", "id")[:100]
        ],
        "account_types": _account_type_options(locale),
        "currencies": currencies,
    }


def _ensure_unique_account_label(user: TelegramUser, label: str, *, exclude_id: int | None = None) -> None:
    qs = _scoped_accounts_qs(user).filter(is_active=True, label__iexact=label)
    if exclude_id is not None:
        qs = qs.exclude(id=exclude_id)
    if qs.exists():
        raise MiniAppTransactionError("account_label_exists", "An active account with this name already exists.")


def _account_draft_confirmation(action: str, summary: list[str], locale: str) -> dict[str, object]:
    titles = {
        "create": mt("Підтвердьте створення рахунку", "Confirm account creation", locale),
        "rename": mt("Підтвердьте перейменування", "Confirm account rename", locale),
        "archive": mt("Підтвердьте архівацію рахунку", "Confirm account archive", locale),
        "balance_correction": mt("Підтвердьте корекцію балансу", "Confirm balance correction", locale),
        "goal_update": mt("Підтвердьте оновлення цілі", "Confirm goal update", locale),
    }
    return {"title": titles[action], "summary": summary}


def build_account_draft(user: TelegramUser, payload: dict[str, object], *, draft_id: str) -> dict[str, object]:
    locale = locale_for_user(user)
    action = _parse_account_action(payload.get("action"))

    if action == "create":
        label = _parse_account_label(payload.get("label"))
        _ensure_unique_account_label(user, label)
        currency = _parse_currency_code(payload.get("currency"), default=_base_currency(user))
        requested_account_type = _normalize_account_type(payload.get("account_type") or "main")
        starting_balance = _parse_signed_transaction_amount(payload.get("starting_balance"), default=ZERO)
        credit_limit = _parse_signed_transaction_amount(payload.get("credit_limit"), default=ZERO)
        if credit_limit < ZERO:
            raise MiniAppTransactionError("invalid_credit_limit", "Credit limit cannot be negative.")
        monthly_rate = _parse_optional_rate(payload.get("monthly_interest_rate"))
        goal_name = str(payload.get("goal_name") or "").strip()[:120]
        goal_amount = _parse_signed_transaction_amount(payload.get("goal_amount"), default=ZERO)
        if goal_amount < ZERO:
            raise MiniAppTransactionError("invalid_goal_amount", "Goal amount cannot be negative.")
        if goal_amount > ZERO and requested_account_type not in GOAL_TYPES:
            raise MiniAppTransactionError("goal_not_supported", "Goals are available for savings, deposit, and investment accounts.")
        goal_date_raw = str(payload.get("goal_date") or "").strip()
        goal_date = _parse_transaction_date(goal_date_raw) if goal_date_raw else None
        non_negative_type = _default_non_negative_account_type(requested_account_type)
        runtime_type = _resolve_runtime_account_type(
            balance=starting_balance,
            non_negative_account_type=non_negative_type,
            current_account_type=requested_account_type,
        )
        account = {
            "label": label,
            "currency": currency,
            "account_type": runtime_type,
            "requested_account_type": requested_account_type,
            "starting_balance": f"{starting_balance:.2f}",
            "credit_limit": f"{credit_limit:.2f}" if credit_limit > ZERO else None,
            "monthly_interest_rate": f"{monthly_rate:.4f}" if monthly_rate is not None else None,
            "non_negative_account_type": non_negative_type,
            "goal_name": goal_name or None,
            "goal_amount": f"{goal_amount:.2f}" if goal_amount > ZERO else None,
            "goal_date": goal_date.isoformat() if goal_date else None,
        }
        return {
            "draft_id": draft_id,
            "action": action,
            "account": account,
            "confirmation": _account_draft_confirmation(
                action,
                [label, money_payload(starting_balance, currency)["display"], _account_type_label(runtime_type, locale=locale)],
                locale,
            ),
        }

    account_id = _parse_positive_int(payload.get("account_id"), code="account_missing", message="Account is required.")
    account = _active_scoped_account(user, account_id)
    if account is None:
        raise MiniAppTransactionError("account_missing", "Account is not available.", status=404)
    account_data = _account_payload(account, locale=locale)

    if action == "rename":
        label = _parse_account_label(payload.get("label"))
        _ensure_unique_account_label(user, label, exclude_id=account_id)
        return {
            "draft_id": draft_id,
            "action": action,
            "account_id": account_id,
            "account": account_data,
            "label": label,
            "confirmation": _account_draft_confirmation(action, [str(account.label), label], locale),
        }

    if action == "archive":
        return {
            "draft_id": draft_id,
            "action": action,
            "account_id": account_id,
            "account": account_data,
            "confirmation": _account_draft_confirmation(
                action,
                [str(account.label), money_payload(account.balance, account.currency)["display"]],
                locale,
            ),
        }

    if action == "balance_correction":
        target_balance = _parse_signed_transaction_amount(payload.get("target_balance"))
        current_balance = _quantize(account.balance)
        delta = _quantize(target_balance - current_balance)
        return {
            "draft_id": draft_id,
            "action": action,
            "account_id": account_id,
            "account": account_data,
            "target_balance": f"{target_balance:.2f}",
            "delta": money_payload(delta, account.currency),
            "confirmation": _account_draft_confirmation(
                action,
                [str(account.label), money_payload(current_balance, account.currency)["display"], money_payload(target_balance, account.currency)["display"]],
                locale,
            ),
        }

    if _goal_account_type(account) not in GOAL_TYPES:
        raise MiniAppTransactionError("goal_not_supported", "Goals are available for savings, deposit, and investment accounts.")
    goal_name = str(payload.get("goal_name") or account.goal_name or account.label or "").strip()[:120]
    goal_amount = _parse_signed_transaction_amount(payload.get("goal_amount"), default=ZERO)
    if goal_amount < ZERO:
        raise MiniAppTransactionError("invalid_goal_amount", "Goal amount cannot be negative.")
    goal_date_raw = str(payload.get("goal_date") or "").strip()
    goal_date = _parse_transaction_date(goal_date_raw) if goal_date_raw else None
    return {
        "draft_id": draft_id,
        "action": action,
        "account_id": account_id,
        "account": account_data,
        "goal_name": goal_name if goal_amount > ZERO else None,
        "goal_amount": f"{goal_amount:.2f}" if goal_amount > ZERO else None,
        "goal_date": goal_date.isoformat() if goal_amount > ZERO and goal_date else None,
        "confirmation": _account_draft_confirmation(
            action,
            [str(account.label), money_payload(goal_amount, account.currency)["display"] if goal_amount > ZERO else mt("Без цілі", "No goal", locale)],
            locale,
        ),
    }


def commit_account_draft(user: TelegramUser, draft: dict[str, object]) -> dict[str, object]:
    locale = locale_for_user(user)
    action = _parse_account_action(draft.get("action"))

    with db_transaction.atomic():
        scope = _lock_finance_scope(user)
        if action == "create":
            account_data = draft.get("account") or {}
            label = _parse_account_label(account_data.get("label"))
            _ensure_unique_account_label(user, label)
            currency = _parse_currency_code(account_data.get("currency"))
            requested_type = _normalize_account_type(account_data.get("requested_account_type") or account_data.get("account_type"))
            starting_balance = _parse_signed_transaction_amount(account_data.get("starting_balance"), default=ZERO)
            credit_limit = _parse_signed_transaction_amount(account_data.get("credit_limit"), default=ZERO)
            if credit_limit < ZERO:
                raise MiniAppTransactionError("invalid_credit_limit", "Credit limit cannot be negative.")
            monthly_rate = _parse_optional_rate(account_data.get("monthly_interest_rate"))
            non_negative_type = _default_non_negative_account_type(account_data.get("non_negative_account_type") or requested_type)
            runtime_type = _resolve_runtime_account_type(
                balance=starting_balance,
                non_negative_account_type=non_negative_type,
                current_account_type=requested_type,
            )
            goal_amount = _parse_signed_transaction_amount(account_data.get("goal_amount"), default=ZERO)
            if goal_amount > ZERO and requested_type not in GOAL_TYPES:
                raise MiniAppTransactionError("goal_not_supported", "Goals are available for savings, deposit, and investment accounts.")
            goal_date = _parse_transaction_date(account_data.get("goal_date")) if account_data.get("goal_date") else None
            account = Account.objects.create(
                tg_user=user,
                family_id=scope.family_id if scope.is_family else None,
                label=label,
                currency=currency,
                account_type=runtime_type,
                starting_balance=starting_balance,
                balance=starting_balance,
                credit_limit=credit_limit if credit_limit > ZERO else None,
                monthly_interest_rate=monthly_rate,
                non_negative_account_type=non_negative_type,
                goal_name=str(account_data.get("goal_name") or "").strip() or None,
                goal_amount=goal_amount if goal_amount > ZERO else None,
                goal_date=goal_date if goal_amount > ZERO else None,
                is_active=True,
                created_at=timezone.now(),
                updated_at=timezone.now(),
            )
            return {"status": "completed", "action": action, "account": _account_payload(account, locale=locale), "dashboard_refresh": True}

        account_id = _parse_positive_int(draft.get("account_id"), code="account_missing", message="Account is required.")
        account = _active_scoped_account(user, account_id, for_update=True)
        if account is None:
            raise MiniAppTransactionError("account_missing", "Account is not available.", status=404)

        if action == "rename":
            label = _parse_account_label(draft.get("label"))
            _ensure_unique_account_label(user, label, exclude_id=account_id)
            account.label = label
            account.save(update_fields=["label", "updated_at"])
            return {"status": "completed", "action": action, "account": _account_payload(account, locale=locale), "dashboard_refresh": True}

        if action == "archive":
            account.is_active = False
            account.save(update_fields=["is_active", "updated_at"])
            return {"status": "completed", "action": action, "account_id": account_id, "dashboard_refresh": True}

        if action == "balance_correction":
            target_balance = _parse_signed_transaction_amount(draft.get("target_balance"))
            previous_balance = _quantize(account.balance)
            delta = _quantize(target_balance - previous_balance)
            current_type = _normalize_account_type(account.account_type)
            non_negative_type = _default_non_negative_account_type(account.non_negative_account_type or current_type)
            next_type = _resolve_runtime_account_type(
                balance=target_balance,
                non_negative_account_type=non_negative_type,
                current_account_type=current_type,
            )
            if delta != ZERO:
                tx = Transaction.objects.create(
                    tg_user=user,
                    created_by_user_id=user.tg_user_id,
                    family_id=scope.family_id if scope.is_family else None,
                    date=timezone.localdate(),
                    type="income" if delta > ZERO else "expense",
                    amount=abs(delta),
                    currency=_normalize_currency(account.currency),
                    comment=mt("Корекція балансу", "Balance correction", locale),
                    source=MINIAPP_BALANCE_CORRECTION_SOURCE,
                    account=account,
                    flow_kind="adjustment",
                    created_at=timezone.now(),
                )
            else:
                tx = None
            account.balance = target_balance
            account.account_type = next_type
            account.non_negative_account_type = non_negative_type
            account.save(update_fields=["balance", "account_type", "non_negative_account_type", "updated_at"])
            return {
                "status": "noop" if tx is None else "completed",
                "action": action,
                "account": _account_payload(account, locale=locale),
                "transaction_id": int(tx.id) if tx is not None else None,
                "dashboard_refresh": True,
            }

        goal_amount = _parse_signed_transaction_amount(draft.get("goal_amount"), default=ZERO)
        goal_date = _parse_transaction_date(draft.get("goal_date")) if draft.get("goal_date") else None
        account.goal_name = str(draft.get("goal_name") or "").strip() or None
        account.goal_amount = goal_amount if goal_amount > ZERO else None
        account.goal_date = goal_date if goal_amount > ZERO else None
        account.save(update_fields=["goal_name", "goal_amount", "goal_date", "updated_at"])
        return {"status": "completed", "action": action, "account": _account_payload(account, locale=locale), "dashboard_refresh": True}


def _transfer_target_amount(source_currency: str, target_currency: str, amount: Decimal, rate: Decimal | None) -> Decimal:
    if source_currency == target_currency:
        return amount
    if rate is None:
        raise MiniAppTransactionError("fx_rate_required", "A manual exchange rate is required for this transfer.")
    return _parse_transaction_amount(amount / rate if source_currency == "UAH" else amount * rate)


def _transfer_rate_text(source_currency: str, target_currency: str, rate: Decimal | None) -> str | None:
    if rate is None:
        return None
    if source_currency == "UAH" and target_currency != "UAH":
        return f"1 {target_currency} = {rate:.4f} UAH"
    return f"1 {source_currency} = {rate:.4f} {target_currency}"


def _validate_transfer_credit(
    *,
    source: Account,
    source_new_balance: Decimal,
    payload_credit_limit: Decimal | None,
) -> Decimal | None:
    source_type = _normalize_account_type(source.account_type)
    current_limit = _quantize(source.credit_limit) if source.credit_limit is not None else None
    effective_limit = payload_credit_limit or current_limit
    if source_new_balance >= ZERO:
        return effective_limit
    if source_type != CREDIT_ACCOUNT_TYPE and effective_limit is None:
        raise MiniAppTransactionError(
            "credit_limit_required",
            "Set a credit limit before this transfer creates a negative balance.",
            extra={"projected_balance": money_payload(source_new_balance, source.currency)},
        )
    if effective_limit is None or abs(source_new_balance) > effective_limit:
        raise MiniAppTransactionError(
            "credit_limit_exceeded",
            "This transfer exceeds the account credit limit.",
            extra={
                "projected_balance": money_payload(source_new_balance, source.currency),
                "credit_limit": money_payload(effective_limit or ZERO, source.currency),
            },
        )
    return effective_limit


def build_transfer_draft(user: TelegramUser, payload: dict[str, object], *, draft_id: str) -> dict[str, object]:
    locale = locale_for_user(user)
    source_id = _parse_positive_int(payload.get("source_account_id"), code="source_account_missing", message="Source account is required.")
    target_id = _parse_positive_int(payload.get("target_account_id"), code="target_account_missing", message="Target account is required.")
    if source_id == target_id:
        raise MiniAppTransactionError("same_account", "Choose two different accounts.")
    source = _active_scoped_account(user, source_id)
    target = _active_scoped_account(user, target_id)
    if source is None or target is None:
        raise MiniAppTransactionError("account_missing", "One of the accounts is not available.", status=404)
    amount = _parse_transaction_amount(payload.get("amount"))
    source_currency = _normalize_currency(source.currency)
    target_currency = _normalize_currency(target.currency)
    rate = _parse_optional_rate(payload.get("fx_rate"))
    if source_currency == target_currency:
        rate = None
    target_amount = _transfer_target_amount(source_currency, target_currency, amount, rate)
    source_new_balance = _bounded_decimal(source.balance - amount)
    target_new_balance = _bounded_decimal(target.balance + target_amount)
    credit_limit = _parse_optional_credit_limit(payload.get("credit_limit"))
    effective_credit_limit = _validate_transfer_credit(
        source=source,
        source_new_balance=source_new_balance,
        payload_credit_limit=credit_limit,
    )
    tx_date = _parse_transaction_date(payload.get("transaction_date") or payload.get("date"))
    comment = str(payload.get("comment") or "").strip()[:500]
    transfer_subtype = "credit_payment" if _normalize_account_type(target.account_type) == CREDIT_ACCOUNT_TYPE else None
    return {
        "draft_id": draft_id,
        "source_account_id": source_id,
        "target_account_id": target_id,
        "source_account": _account_payload(source, locale=locale),
        "target_account": _account_payload(target, locale=locale),
        "amount_value": f"{amount:.2f}",
        "target_amount_value": f"{target_amount:.2f}",
        "source_currency": source_currency,
        "target_currency": target_currency,
        "fx_rate": f"{rate:.4f}" if rate is not None else None,
        "fx_rate_text": _transfer_rate_text(source_currency, target_currency, rate),
        "credit_limit": f"{effective_credit_limit:.2f}" if effective_credit_limit is not None else None,
        "transfer_subtype": transfer_subtype,
        "transaction_date": tx_date.isoformat(),
        "comment": comment,
        "confirmation": {
            "title": mt("Підтвердьте переказ", "Confirm transfer", locale),
            "summary": [
                money_payload(amount, source_currency)["display"],
                f"{source.label} → {target.label}",
                money_payload(target_amount, target_currency)["display"],
            ],
        },
    }


def commit_transfer_draft(user: TelegramUser, draft: dict[str, object]) -> dict[str, object]:
    locale = locale_for_user(user)
    source_id = _parse_positive_int(draft.get("source_account_id"), code="source_account_missing", message="Source account is required.")
    target_id = _parse_positive_int(draft.get("target_account_id"), code="target_account_missing", message="Target account is required.")
    if source_id == target_id:
        raise MiniAppTransactionError("same_account", "Choose two different accounts.")
    amount = _parse_transaction_amount(draft.get("amount_value"))
    rate = _parse_optional_rate(draft.get("fx_rate"))
    requested_credit_limit = _parse_optional_rate(draft.get("credit_limit"))
    tx_date = _parse_transaction_date(draft.get("transaction_date"))
    comment = str(draft.get("comment") or "").strip()[:500] or None
    scope = _resolve_finance_scope(user)

    with db_transaction.atomic():
        locked_accounts = list(
            _scoped_accounts_qs(user)
            .filter(is_active=True, id__in=[source_id, target_id])
            .select_for_update()
            .order_by("id")
        )
        accounts_by_id = {int(account.id): account for account in locked_accounts}
        source = accounts_by_id.get(source_id)
        target = accounts_by_id.get(target_id)
        if source is None or target is None:
            raise MiniAppTransactionError("account_missing", "One of the accounts is not available.", status=404)
        source_currency = _normalize_currency(source.currency)
        target_currency = _normalize_currency(target.currency)
        if draft.get("source_currency") != source_currency or draft.get("target_currency") != target_currency:
            raise MiniAppTransactionError("transfer_changed", "The reviewed currencies changed. Create a new preview.", status=409)
        if source_currency == target_currency:
            rate = None
        target_amount = _transfer_target_amount(source_currency, target_currency, amount, rate)
        if _parse_transaction_amount(draft.get("target_amount_value")) != target_amount:
            raise MiniAppTransactionError("transfer_changed", "The reviewed target amount changed. Create a new preview.", status=409)
        source_new_balance = _bounded_decimal(source.balance - amount)
        target_new_balance = _bounded_decimal(target.balance + target_amount)
        effective_credit_limit = _validate_transfer_credit(
            source=source,
            source_new_balance=source_new_balance,
            payload_credit_limit=requested_credit_limit,
        )
        source_type = _normalize_account_type(source.account_type)
        target_type = _normalize_account_type(target.account_type)
        source_non_negative_type = _default_non_negative_account_type(source.non_negative_account_type or source_type)
        target_non_negative_type = _default_non_negative_account_type(target.non_negative_account_type or target_type)
        source_next_type = _resolve_runtime_account_type(
            balance=source_new_balance,
            non_negative_account_type=source_non_negative_type,
            current_account_type=source_type,
        )
        target_next_type = _resolve_runtime_account_type(
            balance=target_new_balance,
            non_negative_account_type=target_non_negative_type,
            current_account_type=target_type,
        )
        source.balance = source_new_balance
        source.account_type = source_next_type
        source.non_negative_account_type = source_non_negative_type
        if effective_credit_limit is not None:
            source.credit_limit = effective_credit_limit
        source.save(update_fields=["balance", "account_type", "non_negative_account_type", "credit_limit", "updated_at"])
        target.balance = target_new_balance
        target.account_type = target_next_type
        target.non_negative_account_type = target_non_negative_type
        target.save(update_fields=["balance", "account_type", "non_negative_account_type", "updated_at"])
        transfer_subtype = "credit_payment" if target_type == CREDIT_ACCOUNT_TYPE else None
        tx = Transaction.objects.create(
            tg_user=user,
            created_by_user_id=user.tg_user_id,
            family_id=scope.family_id if scope.is_family else None,
            date=tx_date,
            type="transfer",
            amount=amount,
            currency=source_currency,
            to_amount=target_amount,
            to_currency=target_currency,
            fx_rate=rate,
            fx_rate_text=_transfer_rate_text(source_currency, target_currency, rate),
            fx_rate_source="manual" if rate is not None else None,
            comment=comment,
            source=MINIAPP_TRANSFER_SOURCE,
            from_account=source,
            to_account=target,
            flow_kind="transfer",
            transfer_subtype=transfer_subtype,
            created_at=timezone.now(),
        )

    return {
        "status": "completed",
        "transfer": {
            "id": int(tx.id),
            "amount": money_payload(amount, source_currency),
            "target_amount": money_payload(target_amount, target_currency),
            "fx_rate_text": tx.fx_rate_text,
            "transfer_subtype": transfer_subtype,
        },
        "source_account": _account_payload(source, locale=locale),
        "target_account": _account_payload(target, locale=locale),
        "dashboard_refresh": True,
    }


def _parse_debt_action(value: object) -> str:
    action = str(value or "").strip().lower()
    if action not in DEBT_ACTIONS:
        raise MiniAppTransactionError("invalid_debt_action", "Debt action is not supported.")
    return action


def _parse_debt_direction(value: object) -> str:
    direction = str(value or "").strip().lower()
    if direction not in {"receivable", "payable"}:
        raise MiniAppTransactionError("invalid_debt_direction", "Debt direction must be receivable or payable.")
    return direction


def _parse_counterparty(value: object) -> str:
    counterparty = str(value or "").strip()
    if not counterparty:
        raise MiniAppTransactionError("counterparty_required", "Counterparty is required.")
    if len(counterparty) > 120:
        raise MiniAppTransactionError("counterparty_too_long", "Counterparty name is too long.")
    return counterparty


def _parse_optional_money(value: object) -> Decimal | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return _parse_transaction_amount(raw)


def _debt_direction_label(direction: str, locale: str) -> str:
    return mt("Мені винні", "Owed to me", locale) if direction == "receivable" else mt("Я винен", "I owe", locale)


def _debt_status_from_amounts(*, initial_amount: Decimal, paid_amount: Decimal, current_status: str | None = None) -> str:
    if str(current_status or "").strip().lower() == "cancelled":
        return "cancelled"
    if initial_amount - paid_amount <= ZERO:
        return "closed"
    return "partially_paid" if paid_amount > ZERO else "active"


def _debt_payload(debt: Debt, *, locale: str) -> dict[str, object]:
    account = debt.account
    return {
        "id": int(debt.id),
        "counterparty_name": str(debt.counterparty_name or ""),
        "direction": str(debt.direction or ""),
        "direction_label": _debt_direction_label(str(debt.direction or ""), locale),
        "initial_amount": money_payload(debt.initial_amount, debt.currency),
        "paid_amount": money_payload(debt.paid_amount, debt.currency),
        "remaining_amount": money_payload(debt.remaining_amount, debt.currency),
        "currency": _normalize_currency(debt.currency),
        "account": _account_payload(account, locale=locale) if account is not None else None,
        "status": str(debt.status or ""),
        "due_date": debt.due_date.isoformat() if debt.due_date else None,
        "comment": str(debt.comment or "") or None,
        "created_at": debt.created_at.isoformat() if debt.created_at else None,
        "updated_at": debt.updated_at.isoformat() if debt.updated_at else None,
        "closed_at": debt.closed_at.isoformat() if debt.closed_at else None,
    }


def _active_scoped_debt(user: TelegramUser, debt_id: int, *, for_update: bool = False) -> Debt | None:
    qs = _scoped_debts_qs(user).select_related("account").filter(id=debt_id)
    if for_update:
        qs = qs.select_for_update(of=("self",))
    return qs.first()


def _resolve_debt_account_amount(
    *,
    debt_amount: Decimal,
    debt_currency: str,
    account_currency: str,
    account_amount_raw: object,
    fx_rate_raw: object,
) -> tuple[Decimal, Decimal | None]:
    normalized_debt_currency = _normalize_currency(debt_currency)
    normalized_account_currency = _normalize_currency(account_currency)
    if normalized_debt_currency == normalized_account_currency:
        return debt_amount, None

    account_amount = _parse_optional_money(account_amount_raw)
    rate = _parse_optional_rate(fx_rate_raw)
    if account_amount is not None:
        resolved_rate = _parse_optional_rate(account_amount / debt_amount)
        return account_amount, resolved_rate
    if rate is None:
        raise MiniAppTransactionError(
            "fx_details_required",
            "Enter either the account amount or a manual exchange rate for a cross-currency debt operation.",
        )
    return _transfer_target_amount(normalized_debt_currency, normalized_account_currency, debt_amount, rate), rate


def _debt_draft_confirmation(action: str, summary: list[str], locale: str) -> dict[str, object]:
    titles = {
        "create": mt("Підтвердьте створення боргу", "Confirm debt creation", locale),
        "repay": mt("Підтвердьте погашення боргу", "Confirm debt repayment", locale),
        "edit": mt("Підтвердьте оновлення боргу", "Confirm debt update", locale),
        "close": mt("Підтвердьте закриття боргу", "Confirm debt closure", locale),
    }
    return {"title": titles[action], "summary": summary}


def build_debt_options(user: TelegramUser) -> dict[str, object]:
    locale = locale_for_user(user)
    options = build_account_options(user)
    debts = _scoped_debts_qs(user).select_related("account").filter(status__in=ACTIVE_DEBT_STATUSES).order_by("-updated_at", "-id")[:100]
    options["debts"] = [_debt_payload(debt, locale=locale) for debt in debts]
    return options


def build_debt_draft(user: TelegramUser, payload: dict[str, object], *, draft_id: str) -> dict[str, object]:
    locale = locale_for_user(user)
    action = _parse_debt_action(payload.get("action"))

    if action == "create":
        direction = _parse_debt_direction(payload.get("direction"))
        counterparty = _parse_counterparty(payload.get("counterparty_name"))
        debt_amount = _parse_transaction_amount(payload.get("amount"))
        debt_currency = _parse_currency_code(payload.get("currency"), default=_base_currency(user))
        account_id = _parse_positive_int(payload.get("account_id"), code="account_missing", message="Account is required.")
        account = _active_scoped_account(user, account_id)
        if account is None:
            raise MiniAppTransactionError("account_missing", "Account is not available.", status=404)
        account_amount, exchange_rate = _resolve_debt_account_amount(
            debt_amount=debt_amount,
            debt_currency=debt_currency,
            account_currency=account.currency,
            account_amount_raw=payload.get("account_amount"),
            fx_rate_raw=payload.get("fx_rate"),
        )
        due_date_raw = str(payload.get("due_date") or "").strip()
        due_date = _parse_transaction_date(due_date_raw) if due_date_raw else None
        transaction_date = _parse_transaction_date(payload.get("transaction_date"))
        comment = str(payload.get("comment") or "").strip()[:500] or None
        new_balance = _validate_debt_balance(account, account.balance - account_amount if direction == "receivable" else account.balance + account_amount)
        return {
            "draft_id": draft_id,
            "action": action,
            "direction": direction,
            "counterparty_name": counterparty,
            "amount": f"{debt_amount:.2f}",
            "currency": debt_currency,
            "account_id": account_id,
            "account_amount": f"{account_amount:.2f}",
            "exchange_rate": f"{exchange_rate:.4f}" if exchange_rate is not None else None,
            "due_date": due_date.isoformat() if due_date else None,
            "transaction_date": transaction_date.isoformat(),
            "comment": comment,
            "account": _account_payload(account, locale=locale),
            "projected_balance": money_payload(new_balance, account.currency),
            "confirmation": _debt_draft_confirmation(
                action,
                [_debt_direction_label(direction, locale), counterparty, money_payload(debt_amount, debt_currency)["display"], account.label],
                locale,
            ),
        }

    debt_id = _parse_positive_int(payload.get("debt_id"), code="debt_missing", message="Debt is required.")
    debt = _active_scoped_debt(user, debt_id)
    if debt is None:
        raise MiniAppTransactionError("debt_missing", "Debt is not available.", status=404)
    if str(debt.status or "").strip().lower() not in ACTIVE_DEBT_STATUSES:
        raise MiniAppTransactionError("debt_locked", "This debt is already closed or cancelled.")

    if action == "close":
        return {
            "draft_id": draft_id,
            "action": action,
            "debt_id": debt_id,
            "debt": _debt_payload(debt, locale=locale),
            "confirmation": _debt_draft_confirmation(
                action,
                [str(debt.counterparty_name), money_payload(debt.remaining_amount, debt.currency)["display"]],
                locale,
            ),
        }

    if action == "edit":
        counterparty = _parse_counterparty(payload.get("counterparty_name") or debt.counterparty_name)
        initial_amount = _parse_transaction_amount(payload.get("amount") or debt.initial_amount)
        paid_amount = _quantize(debt.paid_amount)
        if initial_amount < paid_amount:
            raise MiniAppTransactionError("amount_below_paid", "Debt amount cannot be less than the amount already paid.")
        currency = _parse_currency_code(payload.get("currency"), default=_normalize_currency(debt.currency))
        _validate_debt_currency_edit(debt, currency)
        due_date_raw = str(payload.get("due_date") or "").strip()
        due_date = _parse_transaction_date(due_date_raw) if due_date_raw else None
        comment = str(payload.get("comment") or "").strip()[:500] or None
        remaining_amount = _quantize(initial_amount - paid_amount)
        return {
            "draft_id": draft_id,
            "action": action,
            "debt_id": debt_id,
            "counterparty_name": counterparty,
            "amount": f"{initial_amount:.2f}",
            "currency": currency,
            "due_date": due_date.isoformat() if due_date else None,
            "comment": comment,
            "debt": _debt_payload(debt, locale=locale),
            "remaining_amount": money_payload(remaining_amount, currency),
            "confirmation": _debt_draft_confirmation(
                action,
                [counterparty, money_payload(initial_amount, currency)["display"], money_payload(remaining_amount, currency)["display"]],
                locale,
            ),
        }

    payment_amount = _parse_transaction_amount(payload.get("amount"))
    if payment_amount > _quantize(debt.remaining_amount):
        raise MiniAppTransactionError("debt_amount_exceeded", "Repayment cannot exceed the current debt.")
    account_id = _parse_positive_int(payload.get("account_id"), code="account_missing", message="Account is required.")
    account = _active_scoped_account(user, account_id)
    if account is None:
        raise MiniAppTransactionError("account_missing", "Account is not available.", status=404)
    account_amount, exchange_rate = _resolve_debt_account_amount(
        debt_amount=payment_amount,
        debt_currency=debt.currency,
        account_currency=account.currency,
        account_amount_raw=payload.get("account_amount"),
        fx_rate_raw=payload.get("fx_rate"),
    )
    transaction_date = _parse_transaction_date(payload.get("transaction_date"))
    comment = str(payload.get("comment") or "").strip()[:500] or None
    direction = _parse_debt_direction(debt.direction)
    new_balance = _validate_debt_balance(account, account.balance + account_amount if direction == "receivable" else account.balance - account_amount)
    return {
        "draft_id": draft_id,
        "action": action,
        "debt_id": debt_id,
        "amount": f"{payment_amount:.2f}",
        "currency": _normalize_currency(debt.currency),
        "account_id": account_id,
        "account_amount": f"{account_amount:.2f}",
        "exchange_rate": f"{exchange_rate:.4f}" if exchange_rate is not None else None,
        "transaction_date": transaction_date.isoformat(),
        "comment": comment,
        "debt": _debt_payload(debt, locale=locale),
        "account": _account_payload(account, locale=locale),
        "projected_balance": money_payload(new_balance, account.currency),
        "confirmation": _debt_draft_confirmation(
            action,
            [str(debt.counterparty_name), money_payload(payment_amount, debt.currency)["display"], account.label],
            locale,
        ),
    }


def _save_debt_account_balance(account: Account, new_balance: Decimal) -> None:
    current_type = _normalize_account_type(account.account_type)
    non_negative_type = _default_non_negative_account_type(account.non_negative_account_type or current_type)
    account.balance = new_balance
    account.account_type = _resolve_runtime_account_type(
        balance=new_balance,
        non_negative_account_type=non_negative_type,
        current_account_type=current_type,
    )
    account.non_negative_account_type = non_negative_type
    account.save(update_fields=["balance", "account_type", "non_negative_account_type", "updated_at"])


def _validate_debt_snapshot(debt: Debt, draft: dict[str, object]) -> None:
    reviewed = draft.get("debt") or {}
    current = _debt_payload(debt, locale="en")
    fields = ("id", "direction", "currency", "status", "initial_amount", "paid_amount", "remaining_amount", "counterparty_name", "due_date", "comment", "updated_at", "closed_at")
    if not isinstance(reviewed, dict) or any(reviewed.get(key) != current.get(key) for key in fields):
        raise MiniAppTransactionError("debt_changed", "The debt changed. Review it again before confirming.", status=409)


def _validate_debt_account(account: Account, draft: dict[str, object], amount: Decimal, currency: str, account_amount: Decimal) -> None:
    reviewed = draft.get("account") or {}
    if reviewed.get("currency") != _normalize_currency(account.currency):
        raise MiniAppTransactionError("debt_changed", "The account currency changed. Create a new preview.", status=409)
    if currency == _normalize_currency(account.currency) and amount != account_amount:
        raise MiniAppTransactionError("debt_changed", "The reviewed debt and account amounts do not match.", status=409)


def _validate_debt_balance(account: Account, new_balance: Decimal) -> Decimal:
    new_balance = _bounded_decimal(new_balance)
    if new_balance < min(ZERO, account.balance) and account.credit_limit is not None and abs(new_balance) > _bounded_decimal(account.credit_limit):
        raise MiniAppTransactionError("credit_limit_exceeded", "This debt operation exceeds the account credit limit.")
    return new_balance


def _validate_debt_currency_edit(debt: Debt, currency: str) -> None:
    if currency == _normalize_currency(debt.currency):
        return
    if _quantize(debt.paid_amount) != ZERO or DebtPayment.objects.filter(debt_id=debt.id).exists() or Transaction.all_objects.filter(debt_id=debt.id).exists():
        raise MiniAppTransactionError("debt_currency_locked", "Currency cannot change after a debt payment or ledger event.")


def commit_debt_draft(user: TelegramUser, draft: dict[str, object]) -> dict[str, object]:
    locale = locale_for_user(user)
    action = _parse_debt_action(draft.get("action"))
    scope = _resolve_finance_scope(user)

    with db_transaction.atomic():
        if action == "create":
            direction = _parse_debt_direction(draft.get("direction"))
            counterparty = _parse_counterparty(draft.get("counterparty_name"))
            amount = _parse_transaction_amount(draft.get("amount"))
            currency = _parse_currency_code(draft.get("currency"))
            account_id = _parse_positive_int(draft.get("account_id"), code="account_missing", message="Account is required.")
            account = _active_scoped_account(user, account_id, for_update=True)
            if account is None:
                raise MiniAppTransactionError("account_missing", "Account is not available.", status=404)
            account_amount = _parse_transaction_amount(draft.get("account_amount"))
            exchange_rate = _parse_optional_rate(draft.get("exchange_rate"))
            _validate_debt_account(account, draft, amount, currency, account_amount)
            new_balance = _validate_debt_balance(account, account.balance - account_amount if direction == "receivable" else account.balance + account_amount)
            due_date = _parse_transaction_date(draft.get("due_date")) if draft.get("due_date") else None
            transaction_date = _parse_transaction_date(draft.get("transaction_date"))
            comment = str(draft.get("comment") or "").strip()[:500] or None
            debt = Debt.objects.create(
                tg_user=user,
                family_id=scope.family_id if scope.is_family else None,
                counterparty_name=counterparty,
                direction=direction,
                initial_amount=amount,
                paid_amount=ZERO,
                remaining_amount=amount,
                currency=currency,
                account=account,
                status="active",
                due_date=due_date,
                comment=comment,
                created_at=timezone.now(),
                updated_at=timezone.now(),
            )
            _save_debt_account_balance(account, new_balance)
            tx = Transaction.objects.create(
                tg_user=user,
                created_by_user_id=user.tg_user_id,
                family_id=scope.family_id if scope.is_family else None,
                date=transaction_date,
                type="debt_given" if direction == "receivable" else "debt_received",
                amount=account_amount,
                currency=_normalize_currency(account.currency),
                comment=comment,
                source=MINIAPP_DEBT_SOURCE,
                account=account,
                flow_kind="debt",
                counterparty=counterparty,
                debt=debt,
                debt_action="lend" if direction == "receivable" else "borrow",
                original_amount=amount,
                original_currency=currency,
                exchange_rate=exchange_rate,
                created_at=timezone.now(),
            )
            return {
                "status": "completed",
                "action": action,
                "debt": _debt_payload(debt, locale=locale),
                "transaction_id": int(tx.id),
                "account": _account_payload(account, locale=locale),
                "dashboard_refresh": True,
            }

        debt_id = _parse_positive_int(draft.get("debt_id"), code="debt_missing", message="Debt is required.")
        debt = _active_scoped_debt(user, debt_id, for_update=True)
        if debt is None:
            raise MiniAppTransactionError("debt_missing", "Debt is not available.", status=404)
        _validate_debt_snapshot(debt, draft)
        if str(debt.status or "").strip().lower() not in ACTIVE_DEBT_STATUSES:
            raise MiniAppTransactionError("debt_locked", "This debt is already closed or cancelled.", status=409)

        if action == "close":
            debt.status = "closed"
            debt.closed_at = debt.closed_at or timezone.now()
            debt.updated_at = timezone.now()
            debt.save(update_fields=["status", "closed_at", "updated_at"])
            return {"status": "completed", "action": action, "debt": _debt_payload(debt, locale=locale), "dashboard_refresh": True}

        if action == "edit":
            counterparty = _parse_counterparty(draft.get("counterparty_name"))
            initial_amount = _parse_transaction_amount(draft.get("amount"))
            paid_amount = _quantize(debt.paid_amount)
            if initial_amount < paid_amount:
                raise MiniAppTransactionError("amount_below_paid", "Debt amount cannot be less than the amount already paid.")
            currency = _parse_currency_code(draft.get("currency"), default=_normalize_currency(debt.currency))
            _validate_debt_currency_edit(debt, currency)
            debt.counterparty_name = counterparty
            debt.initial_amount = initial_amount
            debt.remaining_amount = _quantize(initial_amount - paid_amount)
            debt.currency = currency
            debt.due_date = _parse_transaction_date(draft.get("due_date")) if draft.get("due_date") else None
            debt.comment = str(draft.get("comment") or "").strip()[:500] or None
            debt.status = _debt_status_from_amounts(initial_amount=initial_amount, paid_amount=paid_amount, current_status=debt.status)
            debt.closed_at = timezone.now() if debt.status == "closed" else None
            debt.updated_at = timezone.now()
            debt.save(update_fields=["counterparty_name", "initial_amount", "remaining_amount", "currency", "due_date", "comment", "status", "closed_at", "updated_at"])
            return {"status": "completed", "action": action, "debt": _debt_payload(debt, locale=locale), "dashboard_refresh": True}

        amount = _parse_transaction_amount(draft.get("amount"))
        if amount > _quantize(debt.remaining_amount):
            raise MiniAppTransactionError("debt_changed", "The amount exceeds the current debt. Create a new preview.", status=409)
        account_id = _parse_positive_int(draft.get("account_id"), code="account_missing", message="Account is required.")
        account = _active_scoped_account(user, account_id, for_update=True)
        if account is None:
            raise MiniAppTransactionError("account_missing", "Account is not available.", status=404)
        account_amount = _parse_transaction_amount(draft.get("account_amount"))
        exchange_rate = _parse_optional_rate(draft.get("exchange_rate"))
        _validate_debt_account(account, draft, amount, _normalize_currency(debt.currency), account_amount)
        transaction_date = _parse_transaction_date(draft.get("transaction_date"))
        comment = str(draft.get("comment") or "").strip()[:500] or None
        direction = _parse_debt_direction(debt.direction)
        new_balance = _validate_debt_balance(account, account.balance + account_amount if direction == "receivable" else account.balance - account_amount)
        paid_amount = _quantize(debt.paid_amount + amount)
        debt.paid_amount = paid_amount
        debt.remaining_amount = _quantize(debt.initial_amount - paid_amount)
        debt.status = _debt_status_from_amounts(initial_amount=_quantize(debt.initial_amount), paid_amount=paid_amount, current_status=debt.status)
        debt.closed_at = timezone.now() if debt.status == "closed" else None
        debt.updated_at = timezone.now()
        debt.save(update_fields=["paid_amount", "remaining_amount", "status", "closed_at", "updated_at"])
        payment = DebtPayment.objects.create(
            tg_user=user,
            family_id=scope.family_id if scope.is_family else None,
            debt=debt,
            amount=amount,
            currency=_normalize_currency(debt.currency),
            account=account,
            payment_date=transaction_date,
            comment=comment,
            created_at=timezone.now(),
        )
        _save_debt_account_balance(account, new_balance)
        tx = Transaction.objects.create(
            tg_user=user,
            created_by_user_id=user.tg_user_id,
            family_id=scope.family_id if scope.is_family else None,
            date=transaction_date,
            type="debt_repayment_in" if direction == "receivable" else "debt_repayment_out",
            amount=account_amount,
            currency=_normalize_currency(account.currency),
            comment=comment,
            source=MINIAPP_DEBT_SOURCE,
            account=account,
            flow_kind="debt",
            counterparty=str(debt.counterparty_name),
            debt=debt,
            debt_payment=payment,
            debt_action="lend_repaid" if direction == "receivable" else "borrow_repaid",
            original_amount=amount,
            original_currency=_normalize_currency(debt.currency),
            exchange_rate=exchange_rate,
            created_at=timezone.now(),
        )
        return {
            "status": "completed",
            "action": action,
            "debt": _debt_payload(debt, locale=locale),
            "payment_id": int(payment.id),
            "transaction_id": int(tx.id),
            "account": _account_payload(account, locale=locale),
            "dashboard_refresh": True,
        }


def _normal_transactions(user: TelegramUser, period: PeriodSelection):
    return _scoped_transactions_qs(user).filter(
        flow_kind="normal",
        date__gte=period.date_from,
        date__lte=period.date_to,
    )


def _bucket_mode(period: PeriodSelection) -> str:
    span_days = (period.date_to - period.date_from).days + 1
    if span_days <= 31:
        return "day"
    if span_days <= 90:
        return "week"
    return "month"


def _bucket_label(bucket_mode: str, bucket_start: date) -> str:
    if bucket_mode == "day":
        return bucket_start.strftime("%d.%m")
    if bucket_mode == "week":
        return bucket_start.strftime("%d.%m")
    return bucket_start.strftime("%m.%Y")


def _bucket_start(bucket_mode: str, value: date) -> date:
    if bucket_mode == "day":
        return value
    if bucket_mode == "week":
        return value - timedelta(days=value.weekday())
    return value.replace(day=1)


def _next_bucket_start(bucket_mode: str, value: date) -> date:
    if bucket_mode == "day":
        return value + timedelta(days=1)
    if bucket_mode == "week":
        return value + timedelta(days=7)
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _bucket_dates_for_period(period: PeriodSelection, bucket_mode: str) -> list[date]:
    current = _bucket_start(bucket_mode, period.date_from)
    last = _bucket_start(bucket_mode, period.date_to)
    buckets: list[date] = []
    while current <= last:
        buckets.append(current)
        current = _next_bucket_start(bucket_mode, current)
    return buckets


def _section_state(mode: str, reason: str | None = None, note: str | None = None) -> dict[str, object]:
    return {"mode": mode, "reason": reason, "note": note}


def build_flow_preview_legacy(user: TelegramUser, period: PeriodSelection) -> dict[str, object]:
    base_currency = _base_currency(user)
    period_qs = _normal_transactions(user, period)
    previous_qs = _normal_transactions(user, comparable_previous_period(period))
    foreign_exists = period_qs.exclude(currency__iexact=base_currency).exists()
    filtered_qs = period_qs.filter(currency__iexact=base_currency)

    income_total = _quantize(filtered_qs.filter(type="income").aggregate(total=Sum("amount"))["total"])
    expense_total = _quantize(filtered_qs.filter(type="expense").aggregate(total=Sum("amount"))["total"])
    net_total = income_total - expense_total
    previous_expense_total = _quantize(
        previous_qs.filter(currency__iexact=base_currency, type="expense").aggregate(total=Sum("amount"))["total"]
    )

    rows = list(filtered_qs.values("date", "type", "amount").order_by("date", "id"))
    bucket_mode = _bucket_mode(period)
    grouped: dict[date, dict[str, Decimal]] = defaultdict(lambda: {"income": ZERO, "expense": ZERO})
    for row in rows:
        bucket = _bucket_start(bucket_mode, row["date"])
        grouped[bucket][str(row["type"] or "expense")] += _quantize(row["amount"])

    buckets = []
    for bucket_date in _bucket_dates_for_period(period, bucket_mode) if rows else []:
        income = _quantize(grouped[bucket_date]["income"])
        expense = _quantize(grouped[bucket_date]["expense"])
        buckets.append(
            {
                "label": _bucket_label(bucket_mode, bucket_date),
                "income": f"{income:.2f}",
                "expense": f"{expense:.2f}",
                "net": f"{(income - expense):.2f}",
            }
        )

    state = _section_state("empty", "no_transactions")
    if rows:
        state = _section_state(
            "partial" if foreign_exists else "full",
            "base_currency_only" if foreign_exists else None,
            f"Показано лише операції в {base_currency}." if foreign_exists else None,
        )

    return {
        "state": state,
        "bucket_mode": bucket_mode,
        "income_total": money_payload(income_total, base_currency),
        "expense_total": money_payload(expense_total, base_currency),
        "net_total": money_payload(net_total, base_currency),
        "expense_share_of_income_percent": None if income_total == ZERO else int((expense_total / income_total * ONE_HUNDRED).quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
        "vs_previous_percent": _percent_change(expense_total, previous_expense_total),
        "buckets": buckets,
    }


def build_categories_preview_legacy(user: TelegramUser, period: PeriodSelection, *, limit: int = 5) -> dict[str, object]:
    base_currency = _base_currency(user)
    qs = _normal_transactions(user, period).filter(type="expense")
    previous_qs = _normal_transactions(user, comparable_previous_period(period)).filter(type="expense")
    foreign_exists = qs.exclude(currency__iexact=base_currency).exists()
    filtered_qs = qs.filter(currency__iexact=base_currency)

    current_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for category_name, amount in filtered_qs.values_list("category_name_snapshot", "amount"):
        label = str(category_name or "Без категорії").strip() or "Без категорії"
        current_totals[label] += _quantize(amount)

    previous_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for category_name, amount in previous_qs.filter(currency__iexact=base_currency).values_list("category_name_snapshot", "amount"):
        label = str(category_name or "Без категорії").strip() or "Без категорії"
        previous_totals[label] += _quantize(amount)

    ranked = sorted(current_totals.items(), key=lambda item: item[1], reverse=True)
    total = sum((amount for _, amount in ranked), ZERO)

    items = []
    for index, (name, amount) in enumerate(ranked[:limit]):
        share = 0 if total == ZERO else int((amount / total * ONE_HUNDRED).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        items.append(
            {
                "name": name,
                "amount": money_payload(amount, base_currency),
                "share_percent": share,
                "vs_previous_percent": _percent_change(amount, previous_totals.get(name, ZERO)),
                "color": CATEGORY_COLORS[index % len(CATEGORY_COLORS)],
            }
        )

    if not items:
        state = _section_state("empty", "no_categories")
    else:
        state = _section_state(
            "partial" if foreign_exists else "full",
            "base_currency_only" if foreign_exists else None,
            f"Показано лише витрати в {base_currency}." if foreign_exists else None,
        )

    return {
        "state": state,
        "total": money_payload(total, base_currency),
        "items": items,
    }


def build_flow_preview(user: TelegramUser, period: PeriodSelection) -> dict[str, object]:
    base_currency = _base_currency(user)
    current_rows = _converted_queryset_rows(_normal_transactions(user, period), base_currency=base_currency)
    previous_rows = _converted_queryset_rows(
        _normal_transactions(user, comparable_previous_period(period)),
        base_currency=base_currency,
    )

    rows = current_rows.rows
    income_total = sum((_quantize(row.get("base_amount")) for row in rows if str(row.get("type") or "") == "income"), ZERO)
    expense_total = sum((_quantize(row.get("base_amount")) for row in rows if str(row.get("type") or "") == "expense"), ZERO)
    net_total = income_total - expense_total
    previous_income_total = None
    previous_expense_total = None
    if previous_rows.comparison_available:
        previous_income_total = sum(
            (_quantize(row.get("base_amount")) for row in previous_rows.rows if str(row.get("type") or "") == "income"),
            ZERO,
        )
        previous_expense_total = sum(
            (_quantize(row.get("base_amount")) for row in previous_rows.rows if str(row.get("type") or "") == "expense"),
            ZERO,
        )

    bucket_mode = _bucket_mode(period)
    grouped: dict[date, dict[str, Decimal]] = defaultdict(lambda: {"income": ZERO, "expense": ZERO})
    for row in rows:
        bucket = _bucket_start(bucket_mode, row["date"])
        grouped[bucket][str(row["type"] or "expense")] += _quantize(row.get("base_amount"))

    buckets = []
    for bucket_date in _bucket_dates_for_period(period, bucket_mode) if rows else []:
        income = _quantize(grouped[bucket_date]["income"])
        expense = _quantize(grouped[bucket_date]["expense"])
        buckets.append(
            {
                "label": _bucket_label(bucket_mode, bucket_date),
                "income": f"{income:.2f}",
                "expense": f"{expense:.2f}",
                "net": f"{(income - expense):.2f}",
            }
        )

    state = _section_state("empty", "no_transactions")
    if current_rows.reason == "fx_unavailable":
        state = _section_state(current_rows.mode, current_rows.reason, current_rows.note)
    elif rows:
        note = _merge_notes(
            current_rows.note,
            None if previous_rows.comparison_available else "Порівняння з попереднім періодом тимчасово недоступне через курси НБУ.",
        )
        state = _section_state(
            "partial" if note else current_rows.mode,
            current_rows.reason if current_rows.reason else ("fx_compare_unavailable" if not previous_rows.comparison_available else None),
            note,
        )

    if current_rows.reason == "fx_unavailable":
        income_payload = unavailable_money_payload(currency=base_currency, note=current_rows.note or "Орієнтовний підсумок тимчасово недоступний.")
        expense_payload = unavailable_money_payload(currency=base_currency, note=current_rows.note or "Орієнтовний підсумок тимчасово недоступний.")
        net_payload = unavailable_money_payload(currency=base_currency, note=current_rows.note or "Орієнтовний підсумок тимчасово недоступний.")
        buckets = []
    elif current_rows.reason == "fx_estimated":
        income_payload = estimated_money_payload(income_total, base_currency, note=current_rows.note, is_mixed=False)
        expense_payload = estimated_money_payload(expense_total, base_currency, note=current_rows.note, is_mixed=False)
        net_payload = estimated_money_payload(net_total, base_currency, note=current_rows.note, is_mixed=False)
    else:
        income_payload = money_payload(income_total, base_currency)
        expense_payload = money_payload(expense_total, base_currency)
        net_payload = money_payload(net_total, base_currency)

    return {
        "state": state,
        "bucket_mode": bucket_mode,
        "income_total": income_payload,
        "income_vs_previous_percent": _percent_change(income_total, previous_income_total) if previous_income_total is not None else None,
        "expense_total": expense_payload,
        "net_total": net_payload,
        "expense_share_of_income_percent": None if income_total == ZERO else int((expense_total / income_total * ONE_HUNDRED).quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
        "vs_previous_percent": _percent_change(expense_total, previous_expense_total) if previous_expense_total is not None else None,
        "buckets": buckets,
    }


def build_categories_preview(user: TelegramUser, period: PeriodSelection, *, limit: int = 5) -> dict[str, object]:
    base_currency = _base_currency(user)
    current_rows = _converted_queryset_rows(
        _normal_transactions(user, period).filter(type="expense"),
        base_currency=base_currency,
        extra_fields=("category_name_snapshot", "category__name"),
    )
    previous_rows = _converted_queryset_rows(
        _normal_transactions(user, comparable_previous_period(period)).filter(type="expense"),
        base_currency=base_currency,
        extra_fields=("category_name_snapshot", "category__name"),
    )

    current_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for row in current_rows.rows:
        label = str(row.get("category_name_snapshot") or "Без категорії").strip() or "Без категорії"
        label = str(row.get("category__name") or label).strip() or "Без категорії"
        current_totals[label] += _quantize(row.get("base_amount"))

    previous_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
    if previous_rows.comparison_available:
        for row in previous_rows.rows:
            label = str(row.get("category_name_snapshot") or "Без категорії").strip() or "Без категорії"
            label = str(row.get("category__name") or label).strip() or "Без категорії"
            previous_totals[label] += _quantize(row.get("base_amount"))

    ranked = sorted(current_totals.items(), key=lambda item: item[1], reverse=True)
    total = sum((amount for _, amount in ranked), ZERO)

    items = []
    for index, (name, amount) in enumerate(ranked[:limit]):
        share = 0 if total == ZERO else int((amount / total * ONE_HUNDRED).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        amount_payload = (
            estimated_money_payload(amount, base_currency, note=current_rows.note, is_mixed=False)
            if current_rows.reason == "fx_estimated"
            else money_payload(amount, base_currency)
        )
        items.append(
            {
                "name": name,
                "amount": amount_payload,
                "share_percent": share,
                "vs_previous_percent": _percent_change(amount, previous_totals.get(name, ZERO)) if previous_rows.comparison_available else None,
                "comparison_available": previous_rows.comparison_available,
                "color": CATEGORY_COLORS[index % len(CATEGORY_COLORS)],
            }
        )

    if current_rows.reason == "fx_unavailable":
        state = _section_state(current_rows.mode, current_rows.reason, current_rows.note)
        total_payload = unavailable_money_payload(currency=base_currency, note=current_rows.note or "Орієнтовний підсумок тимчасово недоступний.")
    elif not items:
        state = _section_state("empty", "no_categories")
        total_payload = money_payload(total, base_currency)
    else:
        state_note = _merge_notes(
            current_rows.note,
            None if previous_rows.comparison_available else "Порівняння з попереднім періодом тимчасово недоступне через курси НБУ.",
        )
        state = _section_state(
            "partial" if state_note else current_rows.mode,
            current_rows.reason if current_rows.reason else ("fx_compare_unavailable" if not previous_rows.comparison_available else None),
            state_note,
        )
        total_payload = (
            estimated_money_payload(total, base_currency, note=current_rows.note)
            if current_rows.reason == "fx_estimated"
            else money_payload(total, base_currency)
        )

    return {
        "state": state,
        "total": total_payload,
        "items": items,
    }


def _activity_type_code(tx: Transaction) -> str:
    flow_kind = str(tx.flow_kind or "")
    if flow_kind == "debt":
        return "debt"
    if flow_kind == "transfer":
        return "transfer"
    if flow_kind == "adjustment":
        return "adjustment"
    return "income" if str(tx.type or "") == "income" else "expense"


def _activity_type_badge(tx: Transaction, *, locale: str = "uk") -> str:
    type_code = _activity_type_code(tx)
    labels = {
        "debt": mt("Борг", "Debt", locale),
        "transfer": mt("Переказ", "Transfer", locale),
        "adjustment": mt("Корекція", "Adjustment", locale),
        "income": mt("Дохід", "Income", locale),
        "expense": mt("Витрата", "Expense", locale),
    }
    return labels.get(type_code, mt("Операція", "Transaction", locale))


def _activity_title(tx: Transaction, *, locale: str = "uk") -> str:
    if tx.flow_kind == "debt":
        return f"{mt('Борг', 'Debt', locale)} • {tx.counterparty or mt('Контрагент', 'Counterparty', locale)}"
    if tx.flow_kind == "transfer":
        return mt("Переказ", "Transfer", locale)
    label = str(tx.category_name_snapshot or "").strip()
    if label:
        return label
    if tx.comment:
        return str(tx.comment).strip()
    return mt("Дохід", "Income", locale) if tx.type == "income" else mt("Витрата", "Expense", locale)


def _activity_subtitle(tx: Transaction) -> str:
    parts = [tx.date.strftime("%d.%m.%Y")]
    if tx.flow_kind == "transfer":
        if tx.from_account and tx.to_account:
            parts.append(f"{tx.from_account.label} -> {tx.to_account.label}")
        elif tx.from_account:
            parts.append(tx.from_account.label)
        elif tx.to_account:
            parts.append(tx.to_account.label)
    elif tx.account:
        parts.append(tx.account.label)
    if tx.comment and tx.flow_kind != "normal":
        parts.append(str(tx.comment).strip())
    return " • ".join(part for part in parts if part)


def build_activity_preview(user: TelegramUser, period: PeriodSelection, *, limit: int = 5) -> dict[str, object]:
    locale = locale_for_user(user)
    scope = _resolve_finance_scope(user)
    scoped_qs = _scoped_transactions_qs(user)
    rows = (
        scoped_qs.filter(
            date__gte=period.date_from,
            date__lte=period.date_to,
        )
        .select_related("account", "from_account", "to_account")
        .order_by("-date", "-created_at", "-id")[:limit]
    )

    items = [
        {
            "id": tx.id,
            "title": _activity_title(tx, locale=locale),
            "subtitle": _activity_subtitle(tx),
            "type_badge": _activity_type_badge(tx, locale=locale),
            "type_code": _activity_type_code(tx),
            "amount": money_payload(-tx.amount if tx.type == "expense" and tx.flow_kind == "normal" else tx.amount, tx.currency),
            "voidable": bool(
                tx.flow_kind == "normal"
                and tx.type in {"income", "expense"}
                and tx.account_id is not None
                and (
                    not scope.is_family
                    or str(scope.role or "member").lower() == "owner"
                    or int(tx.created_by_user_id or tx.tg_user_id) == int(user.tg_user_id)
                )
            ),
        }
        for tx in rows
    ]

    empty_reason = None
    if not items:
        empty_reason = "no_activity_in_period" if scoped_qs.exists() else "no_activity"

    return {
        "state": _section_state("full" if items else "empty", None if items else empty_reason),
        "items": items,
    }


def _account_type_label(account_type: str, *, locale: str = "uk") -> str:
    normalized = str(account_type or "").strip().lower()
    labels = {
        "main": mt("Основний", "Main", locale),
        "cash": mt("Готівка", "Cash", locale),
        "credit": mt("Кредитка", "Credit card", locale),
        "investment": mt("Інвестиції", "Investments", locale),
        "savings": mt("Заощадження", "Savings", locale),
        "deposit": mt("Депозит", "Deposit", locale),
        "other": mt("Рахунок", "Account", locale),
        "card": mt("Картка", "Card", locale),
        "bank": mt("Банк", "Bank", locale),
        "crypto": mt("Інвестиції", "Investments", locale),
    }
    return labels.get(normalized, mt("Рахунок", "Account", locale))


def build_accounts_preview(user: TelegramUser, *, limit: int = 8) -> dict[str, object]:
    locale = locale_for_user(user)
    accounts_qs = _scoped_accounts_qs(user).filter(is_active=True).order_by("-balance", "label")
    asset_accounts_qs = accounts_qs.exclude(account_type__in=LIABILITY_ACCOUNT_TYPES)
    items = [
        {
            "id": account.id,
            "label": account.label,
            "account_type": account.account_type,
            "type_label": _account_type_label(account.account_type, locale=locale),
            "currency": account.currency,
            "balance": money_payload(account.balance, account.currency),
        }
        for account in asset_accounts_qs[:limit]
    ]

    totals_rows = list(asset_accounts_qs.values("currency").annotate(total=Sum("balance")).order_by("currency"))
    totals_result = _rows_to_money(totals_rows, preferred_currency=_base_currency(user))
    usd_equivalent_result = _rows_to_usd_equivalent(totals_rows)
    return {
        "state": _section_state(
            totals_result.mode if items else "empty",
            totals_result.reason if items else "no_accounts",
            totals_result.note if items else None,
        ),
        "items": items,
        "total_balance": totals_result.payload if items else money_payload(ZERO, _base_currency(user)),
        "usd_equivalent": usd_equivalent_result.payload if items else money_payload(ZERO, "USD"),
    }


def build_credit_cards_preview(user: TelegramUser, *, limit: int = 6) -> dict[str, object]:
    credit_qs = _scoped_accounts_qs(user).filter(is_active=True, account_type__in=LIABILITY_ACCOUNT_TYPES).order_by("balance", "label")
    rows = list(credit_qs[:limit])
    if not rows:
        return {
            "state": _section_state("empty", "no_credit_cards"),
            "summary": {
                "total_debt": money_payload(ZERO, _base_currency(user)),
                "cards_count": 0,
            },
            "items": [],
        }

    debt_rows: list[dict[str, object]] = []
    items: list[dict[str, object]] = []
    for account in rows:
        balance = _quantize(account.balance)
        debt_amount = abs(balance) if balance < ZERO else ZERO
        credit_limit = _quantize(account.credit_limit) if account.credit_limit is not None else None
        available_credit = None if credit_limit is None else max(ZERO, credit_limit - debt_amount)
        utilization_percent = None
        if credit_limit is not None and credit_limit > ZERO:
            utilization_percent = int((debt_amount / credit_limit * ONE_HUNDRED).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        debt_rows.append({"currency": account.currency, "total": debt_amount})
        items.append(
            {
                "id": account.id,
                "label": account.label,
                "currency": account.currency,
                "balance": money_payload(account.balance, account.currency),
                "debt": money_payload(debt_amount, account.currency),
                "credit_limit": money_payload(credit_limit, account.currency) if credit_limit is not None else None,
                "available_credit": money_payload(available_credit, account.currency) if available_credit is not None else None,
                "utilization_percent": utilization_percent,
                "monthly_interest_rate": (
                    f"{_quantize(account.monthly_interest_rate):.4f}".rstrip("0").rstrip(".")
                    if account.monthly_interest_rate is not None
                    else None
                ),
            }
        )

    total_debt = _rows_to_money(debt_rows, preferred_currency=_base_currency(user))
    return {
        "state": _section_state(total_debt.mode, total_debt.reason, total_debt.note),
        "summary": {
            "total_debt": total_debt.payload,
            "cards_count": credit_qs.count(),
        },
        "items": items,
    }


def build_debts_preview(user: TelegramUser, *, limit: int = 5) -> dict[str, object]:
    debts_qs = _scoped_debts_qs(user).exclude(status__iexact="closed")
    rows = list(debts_qs.values("direction", "currency").annotate(total=Sum("remaining_amount")).order_by("direction", "currency"))

    receivable_rows = [row for row in rows if str(row.get("direction") or "") == "receivable"]
    payable_rows = [row for row in rows if str(row.get("direction") or "") == "payable"]

    items = [
        {
            "counterparty_name": debt.counterparty_name,
            "direction": debt.direction,
            "remaining_amount": money_payload(debt.remaining_amount, debt.currency),
            "status": debt.status,
        }
        for debt in debts_qs.order_by("-updated_at", "-id")[:limit]
    ]

    receivable_total = _rows_to_money(receivable_rows, preferred_currency=_base_currency(user))
    payable_total = _rows_to_money(payable_rows, preferred_currency=_base_currency(user))
    summary_note = _merge_notes(receivable_total.note, payable_total.note)
    summary_reason = receivable_total.reason or payable_total.reason
    summary_mode = "partial" if summary_reason else ("full" if rows else "empty")

    return {
        "state": _section_state(summary_mode, summary_reason if rows else "no_debts", summary_note if rows else None),
        "summary": {
            "owed_to_user": receivable_total.payload,
            "owed_by_user": payable_total.payload,
            "active_count": debts_qs.count(),
        },
        "items": items,
    }


def _table_columns(table_name: str) -> set[str]:
    with connection.cursor() as cursor:
        description = connection.introspection.get_table_description(cursor, table_name)
    return {column.name for column in description}


def build_goals_preview(user: TelegramUser, *, limit: int = 4) -> dict[str, object]:
    locale = locale_for_user(user)
    if "accounts" not in connection.introspection.table_names():
        return {"state": _section_state("empty", "no_accounts_table"), "items": []}

    columns = _table_columns("accounts")
    required = {"goal_name", "goal_amount"}
    if not required.issubset(columns):
        return {
            "state": _section_state(
                "empty",
                "goal_metadata_missing",
                mt(
                    "Цілі з’являться після додавання goal-полів до рахунків.",
                    "Goals will appear after goal fields are added to accounts.",
                    locale,
                ),
            ),
            "items": [],
        }

    goal_date_expr = "goal_date" if "goal_date" in columns else "NULL AS goal_date"
    scope = _resolve_finance_scope(user)
    if scope.is_family:
        scope_condition = "family_id = %s"
        scope_params = [int(scope.family_id)]
    else:
        scope_condition = "tg_user_id = %s AND family_id IS NULL"
        scope_params = [int(user.tg_user_id)]
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT id, label, currency, balance, goal_name, goal_amount, {goal_date_expr}
            FROM accounts
            WHERE {scope_condition}
              AND is_active = true
              AND account_type IN ('savings', 'deposit', 'investment')
              AND goal_name IS NOT NULL
              AND goal_name <> ''
              AND goal_amount IS NOT NULL
            ORDER BY id DESC
            LIMIT %s
            """,
            [*scope_params, int(limit)],
        )
        rows = cursor.fetchall()

    items = []
    for row in rows:
        account_id, label, currency, balance, goal_name, goal_amount, goal_date = row
        current = _quantize(balance)
        target = _quantize(goal_amount)
        progress = 0 if target <= ZERO else int((current / target * ONE_HUNDRED).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        items.append(
            {
                "account_id": int(account_id),
                "label": str(goal_name or label or mt("Ціль", "Goal", locale)),
                "current_amount": money_payload(current, currency),
                "target_amount": money_payload(target, currency),
                "progress_percent": progress,
                "target_date": goal_date.isoformat() if goal_date else None,
            }
        )

    return {
        "state": _section_state("full" if items else "empty", None if items else "no_goals"),
        "items": items,
    }


def build_investments_preview(user: TelegramUser, *, limit: int = 4) -> dict[str, object]:
    locale = locale_for_user(user)
    accounts_qs = _scoped_accounts_qs(user).filter(is_active=True, account_type__in=INVESTMENT_TYPES)
    items = [
        {
            "id": account.id,
            "label": account.label,
            "current_value": money_payload(account.balance, account.currency),
        }
        for account in accounts_qs.order_by("-balance", "label")[:limit]
    ]
    totals_rows = list(accounts_qs.values("currency").annotate(total=Sum("balance")).order_by("currency"))
    totals_result = _rows_to_money(totals_rows, preferred_currency=_base_currency(user))
    if not items:
        return {
            "state": _section_state("empty", "no_investments"),
            "summary": {
                "invested": None,
                "profit": None,
                "current_value": money_payload(ZERO, _base_currency(user)),
            },
            "items": [],
        }
    state_note = _merge_notes(
        mt(
            "Поки що показуємо лише поточний стан інвест-рахунків.",
            "For now, only the current value of investment accounts is shown.",
            locale,
        ),
        totals_result.note,
    )
    return {
        "state": _section_state("partial", totals_result.reason or "current_value_only", state_note),
        "summary": {
            "invested": None,
            "profit": None,
            "current_value": totals_result.payload,
        },
        "items": items,
    }


def _combine_current_debt_payload(
    credit_cards_preview: dict[str, object],
    debts_preview: dict[str, object],
    *,
    preferred_currency: str,
    locale: str,
) -> dict[str, object]:
    credit_payload = credit_cards_preview["summary"]["total_debt"]
    payable_payload = debts_preview["summary"]["owed_by_user"]
    payloads = (credit_payload, payable_payload)
    notes = _merge_notes(*(payload.get("note") for payload in payloads))

    unavailable_payload = next((payload for payload in payloads if payload.get("value") is None), None)
    if unavailable_payload is not None:
        combined = dict(unavailable_payload)
        combined["currency"] = _normalize_currency(preferred_currency)
        combined["note"] = notes
        return combined

    normalized_currency = _normalize_currency(preferred_currency)
    payload_currencies = {
        _normalize_currency(str(payload.get("currency") or ""), default=normalized_currency)
        for payload in payloads
    }
    if payload_currencies != {normalized_currency}:
        note = _merge_notes(
            notes,
            mt(
                "Підсумок боргу тимчасово недоступний через різні валюти.",
                "The debt total is temporarily unavailable because the currencies differ.",
                locale,
            ),
        )
        return unavailable_money_payload(currency=normalized_currency, note=note or "")

    total = sum((_quantize(payload.get("value")) for payload in payloads), ZERO)
    is_estimated = any(bool(payload.get("is_estimated")) for payload in payloads)
    is_mixed = any(bool(payload.get("is_mixed")) for payload in payloads)
    if is_estimated:
        return estimated_money_payload(total, normalized_currency, note=notes, is_mixed=is_mixed)

    combined = money_payload(total, normalized_currency)
    combined["is_mixed"] = is_mixed
    combined["note"] = notes
    return combined


def build_overview_from_previews(
    accounts_preview: dict[str, object],
    flow_preview: dict[str, object],
    credit_cards_preview: dict[str, object],
    debts_preview: dict[str, object],
    *,
    base_currency: str,
    locale: str,
) -> dict[str, object]:
    expenses = flow_preview["expense_total"]
    return {
        "total_balance": accounts_preview["total_balance"],
        "usd_equivalent": accounts_preview.get("usd_equivalent", money_payload(ZERO, "USD")),
        "credit_debt": credit_cards_preview["summary"]["total_debt"],
        "current_debt": _combine_current_debt_payload(
            credit_cards_preview,
            debts_preview,
            preferred_currency=base_currency,
            locale=locale,
        ),
        "net_flow": flow_preview["net_total"],
        "expenses": {
            "total": expenses,
            "vs_previous_percent": flow_preview["vs_previous_percent"],
        },
    }


def build_bootstrap(user: TelegramUser, period: PeriodSelection) -> dict[str, object]:
    access = resolve_access(user)
    flow_preview = build_flow_preview(user, period)
    categories_preview = build_categories_preview(user, period)
    activity_preview = build_activity_preview(user, period)
    accounts_preview = build_accounts_preview(user)
    credit_cards_preview = build_credit_cards_preview(user)
    debts_preview = build_debts_preview(user)
    goals_preview = build_goals_preview(user)
    investments_preview = build_investments_preview(user)

    return {
        "user": {
            "tg_user_id": int(user.tg_user_id),
            "first_name": str(user.first_name or ""),
            "base_currency": _base_currency(user),
            "lang": locale_for_user(user),
        },
        "period": {
            "preset": period.preset,
            "date_from": period.date_from.isoformat(),
            "date_to": period.date_to.isoformat(),
            "label": period.label,
        },
        "access": access,
        "overview": build_overview_from_previews(
            accounts_preview,
            flow_preview,
            credit_cards_preview,
            debts_preview,
            base_currency=_base_currency(user),
            locale=locale_for_user(user),
        ),
        "flow_preview": flow_preview,
        "categories_preview": categories_preview,
        "activity_preview": activity_preview,
        "accounts_preview": accounts_preview,
        "credit_cards_preview": credit_cards_preview,
        "debts_preview": debts_preview,
        "goals_preview": goals_preview,
        "investments_preview": investments_preview,
    }
