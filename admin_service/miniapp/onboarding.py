from __future__ import annotations

from datetime import date

from django.db import transaction as db_transaction
from django.db.models import Q
from django.utils import timezone

from categories.models import Category, CategoryTemplate
from miniapp.services import (
    MiniAppTransactionError,
    _lock_finance_scope,
    _resolve_finance_scope,
    _parse_currency_code,
    _parse_transaction_date,
    _scoped_accounts_qs,
    _scoped_categories_qs,
    build_account_draft,
    build_account_options,
    commit_account_draft,
    locale_for_user,
    mt,
    normalize_locale,
)
from users.models import TelegramUser


CURRENT_ONBOARDING_VERSION = 3
ONBOARDING_ACCOUNT_LIMIT = 15


def _onboarding_accounts(user: TelegramUser):
    return _scoped_accounts_qs(user).filter(is_active=True)


def onboarding_is_complete(user: TelegramUser) -> bool:
    return bool(
        user.onboarding_completed
        and int(user.onboarding_version or 0) >= CURRENT_ONBOARDING_VERSION
        and user.start_date
        and user.base_currency
        and _onboarding_accounts(user).exists()
        and {"income", "expense"}.issubset(set(_scoped_categories_qs(user).filter(is_active=True, deleted_at__isnull=True).values_list("type", flat=True)))
    )


def onboarding_status(user: TelegramUser) -> dict[str, object]:
    options = build_account_options(user)
    return {
        "completed": onboarding_is_complete(user),
        "version": CURRENT_ONBOARDING_VERSION,
        "defaults": {
            "lang": locale_for_user(user),
            "base_currency": str(user.base_currency or "UAH").upper(),
            "start_date": (user.start_date or timezone.localdate()).isoformat(),
        },
        "account_options": {
            "currencies": list(options.get("currencies") or []),
            "account_types": list(options.get("account_types") or []),
        },
        "limits": {"accounts": ONBOARDING_ACCOUNT_LIMIT},
    }


def _parse_onboarding_accounts(user: TelegramUser, payload: dict[str, object], *, draft_id: str) -> list[dict[str, object]]:
    raw_accounts = payload.get("accounts")
    if not isinstance(raw_accounts, list):
        raise MiniAppTransactionError("invalid_onboarding_accounts", "Accounts must be a list.")
    if len(raw_accounts) > ONBOARDING_ACCOUNT_LIMIT:
        raise MiniAppTransactionError("onboarding_account_limit", f"You can add up to {ONBOARDING_ACCOUNT_LIMIT} accounts.")

    drafts: list[dict[str, object]] = []
    labels: set[str] = set()
    for index, raw_account in enumerate(raw_accounts, start=1):
        if not isinstance(raw_account, dict):
            raise MiniAppTransactionError("invalid_onboarding_account", "Each account must be an object.")
        account_payload = dict(raw_account)
        account_payload["action"] = "create"
        account_draft = build_account_draft(user, account_payload, draft_id=f"{draft_id}-{index}")
        account = account_draft.get("account") if isinstance(account_draft.get("account"), dict) else {}
        label = str(account.get("label") or "").casefold()
        if label in labels:
            raise MiniAppTransactionError("account_label_exists", "Each account name must be unique.")
        labels.add(label)
        drafts.append(account_draft)
    return drafts


def _active_templates() -> list[CategoryTemplate]:
    templates = list(
        CategoryTemplate.objects.filter(type__in=["expense", "income"], is_active=True)
        .order_by("type", "sort_order", "name", "id")
    )
    kinds = {str(template.type or "") for template in templates}
    if not {"expense", "income"}.issubset(kinds):
        raise MiniAppTransactionError(
            "onboarding_categories_unavailable",
            "Default categories are temporarily unavailable. Please try again later.",
            status=503,
        )
    return templates


def _ensure_default_categories(user: TelegramUser) -> int:
    templates = _active_templates()
    scope = _resolve_finance_scope(user)
    category_scope = Q(family_id=scope.family_id) if scope.is_family else Q(family_id__isnull=True) & (Q(user_id=user.tg_user_id) | Q(tg_user_id=user.tg_user_id))
    count = 0
    for template in templates:
        category_filter = Q(template_id=template.id)
        if template.slug:
            category_filter |= Q(type=template.type, slug=template.slug)
        else:
            category_filter |= Q(type=template.type, name=template.name)
        category = (
            Category.objects.select_for_update()
            .filter(category_scope)
            .filter(category_filter)
            .order_by("id")
            .first()
        )
        if category is None:
            Category.objects.create(
                # Runtime DDL keeps tg_user_id non-null for legacy bot reads;
                # family_id remains the canonical visibility boundary.
                tg_user=user,
                user=user,
                family_id=scope.family_id if scope.is_family else None,
                created_by_user_id=int(user.tg_user_id),
                template=template,
                kind=template.type,
                type=template.type,
                name=template.name,
                slug=template.slug,
                aliases=list(template.aliases or []),
                source="template",
                is_active=True,
                sort_order=int(template.sort_order or 0),
                created_at=timezone.now(),
                updated_at=timezone.now(),
            )
            count += 1
            continue

        category.template = template
        category.kind = template.type
        category.type = template.type
        category.name = template.name
        category.slug = template.slug
        category.aliases = list(template.aliases or [])
        category.source = "template"
        category.is_active = True
        category.deleted_at = None
        category.sort_order = int(template.sort_order or 0)
        category.save(
            update_fields=[
                "template",
                "kind",
                "type",
                "name",
                "slug",
                "aliases",
                "source",
                "is_active",
                "deleted_at",
                "sort_order",
                "updated_at",
            ]
        )
    return count


def build_onboarding_draft(user: TelegramUser, payload: dict[str, object], *, draft_id: str) -> dict[str, object]:
    locale = normalize_locale(payload.get("lang") or locale_for_user(user))
    base_currency = _parse_currency_code(payload.get("base_currency"), default=str(user.base_currency or "UAH"))
    start_date = _parse_transaction_date(payload.get("start_date") or date.today().isoformat())
    accounts = _parse_onboarding_accounts(user, payload, draft_id=draft_id)
    has_existing_accounts = _onboarding_accounts(user).exists()
    if not accounts and not has_existing_accounts:
        raise MiniAppTransactionError(
            "onboarding_account_required",
            mt("Додай хоча б один рахунок, щоб продовжити.", "Add at least one account before continuing.", locale),
        )
    _active_templates()

    account_summary = []
    for account_draft in accounts:
        account = account_draft.get("account") if isinstance(account_draft.get("account"), dict) else {}
        account_summary.append(
            {
                "label": str(account.get("label") or ""),
                "currency": str(account.get("currency") or base_currency),
                "starting_balance": str(account.get("starting_balance") or "0.00"),
                "account_type": str(account.get("account_type") or "main"),
            }
        )
    return {
        "draft_id": draft_id,
        "lang": locale,
        "base_currency": base_currency,
        "start_date": start_date.isoformat(),
        "accounts": accounts,
        "confirmation": {
            "title": mt("Підтвердьте початкові налаштування", "Confirm initial setup", locale),
            "summary": {
                "base_currency": base_currency,
                "start_date": start_date.isoformat(),
                "accounts": account_summary,
                "default_categories": True,
            },
        },
    }


def commit_onboarding_draft(user: TelegramUser, draft: dict[str, object]) -> dict[str, object]:
    locale = normalize_locale(draft.get("lang") or locale_for_user(user))
    base_currency = _parse_currency_code(draft.get("base_currency"), default=str(user.base_currency or "UAH"))
    start_date = _parse_transaction_date(draft.get("start_date"))
    accounts = draft.get("accounts") if isinstance(draft.get("accounts"), list) else []
    if len(accounts) > ONBOARDING_ACCOUNT_LIMIT:
        raise MiniAppTransactionError("onboarding_account_limit", f"You can add up to {ONBOARDING_ACCOUNT_LIMIT} accounts.")

    with db_transaction.atomic():
        user = TelegramUser.objects.select_for_update().get(tg_user_id=user.tg_user_id)
        _lock_finance_scope(user)
        if onboarding_is_complete(user):
            raise MiniAppTransactionError("onboarding_completed", "Initial setup is already complete.", status=409)
        # Re-check templates before setting the completion flag: a completed user must
        # always have the categories the transaction form depends on.
        categories_created = _ensure_default_categories(user)
        if not {"income", "expense"}.issubset(set(_scoped_categories_qs(user).filter(is_active=True, deleted_at__isnull=True).values_list("type", flat=True))):
            raise MiniAppTransactionError("onboarding_categories_unavailable", "Both default category kinds must be available in this finance scope.", status=503)
        created_accounts: list[dict[str, object]] = []
        for account_draft in accounts:
            if not isinstance(account_draft, dict):
                raise MiniAppTransactionError("invalid_onboarding_account", "Each account must be an object.")
            result = commit_account_draft(user, account_draft)
            account = result.get("account") if isinstance(result.get("account"), dict) else None
            if account is not None:
                created_accounts.append(account)
        if not _onboarding_accounts(user).exists():
            raise MiniAppTransactionError(
                "onboarding_account_required",
                mt("Додай хоча б один рахунок, щоб продовжити.", "Add at least one account before continuing.", locale),
            )

        user.lang = locale
        user.base_currency = base_currency
        user.start_date = start_date
        user.onboarding_completed = True
        user.onboarding_version = CURRENT_ONBOARDING_VERSION
        user.last_seen_at = timezone.now()
        user.save(
            update_fields=[
                "lang",
                "base_currency",
                "start_date",
                "onboarding_completed",
                "onboarding_version",
                "last_seen_at",
            ]
        )

    return {
        "status": "completed",
        "onboarding": onboarding_status(user),
        "accounts": created_accounts,
        "categories_created": categories_created,
    }
