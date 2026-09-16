from __future__ import annotations

import hashlib
from datetime import date, datetime
from decimal import Decimal

from django.core import signing
from django.db import connection, transaction as db_transaction
from django.db.models import Q
from django.utils import timezone

from accounts.models import Account
from categories.models import Category
from transactions.models import Transaction
from users.models import TelegramUser

from miniapp.services import (
    ZERO,
    MiniAppTransactionError,
    _account_payload,
    _active_scoped_category,
    _category_kind,
    _category_payload,
    _default_non_negative_account_type,
    _normalize_account_type,
    _normalize_currency,
    _parse_positive_int,
    _parse_transaction_amount,
    _parse_transaction_date,
    _quantize,
    _resolve_runtime_account_type,
    _resolve_finance_scope,
    _scoped_accounts_qs,
    _scoped_categories_qs,
    _scoped_transactions_qs,
    locale_for_user,
    money_payload,
    mt,
)


class MiniAppHistoryError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def _transaction_signature(transaction: Transaction) -> str:
    payload = "|".join(
        [
            str(transaction.id),
            transaction.date.isoformat() if transaction.date else "",
            str(transaction.type or ""),
            f"{Decimal(str(transaction.amount or 0)):.2f}",
            str(transaction.currency or ""),
            str(transaction.account_id or ""),
            str(transaction.category_id or ""),
            str(transaction.comment or ""),
            str(transaction.flow_kind or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normal_transaction(user: TelegramUser, transaction_id: int, *, lock: bool = False) -> Transaction | None:
    queryset = _scoped_transactions_qs(user).filter(id=transaction_id, flow_kind="normal").select_related("account", "category")
    if lock:
        queryset = queryset.select_for_update(of=("self",))
    return queryset.first()


def _history_item(transaction: Transaction, *, locale: str, voidable: bool | None = None) -> dict[str, object]:
    action_allowed = True if voidable is None else bool(voidable)
    return {
        "id": int(transaction.id),
        "date": transaction.date.isoformat(),
        "kind": str(transaction.type or "expense"),
        "amount": money_payload(transaction.amount, transaction.currency),
        "amount_value": f"{_quantize(transaction.amount):.2f}",
        "currency": _normalize_currency(transaction.currency),
        "comment": str(transaction.comment or ""),
        "account_id": int(transaction.account_id) if transaction.account_id else None,
        "category_id": int(transaction.category_id) if transaction.category_id else None,
        "account": _account_payload(transaction.account, locale=locale) if transaction.account else None,
        "category": _category_payload(transaction.category) if transaction.category else None,
        "editable": action_allowed and transaction.account_id is not None and transaction.category_id is not None,
        "voidable": bool(voidable and transaction.account_id is not None),
    }


def _scoped_all_transactions_qs(user: TelegramUser):
    scope = _resolve_finance_scope(user)
    if scope.is_family:
        return Transaction.all_objects.filter(family_id=scope.family_id)
    return Transaction.all_objects.filter(tg_user_id=user.tg_user_id, family_id__isnull=True)


def _transaction_author_id(transaction: Transaction) -> int:
    return int(transaction.created_by_user_id or transaction.tg_user_id)


def _can_cancel_transaction(user: TelegramUser, transaction: Transaction) -> bool:
    scope = _resolve_finance_scope(user)
    if not scope.is_family:
        return int(transaction.tg_user_id) == int(user.tg_user_id) and transaction.family_id is None
    return str(scope.role or "member").lower() == "owner" or _transaction_author_id(transaction) == int(user.tg_user_id)


def _credit_limit_warning(account: Account, projected_balance: Decimal, *, locale: str) -> dict[str, object] | None:
    if account.credit_limit is None:
        return None
    credit_limit = _quantize(account.credit_limit)
    if projected_balance >= ZERO or abs(projected_balance) <= credit_limit:
        return None
    return {
        "code": "credit_limit_exceeded_after_cancellation",
        "message": mt(
            "Після скасування баланс буде нижчим за кредитний ліміт. Скасування все одно дозволено.",
            "After cancellation the balance will exceed the credit limit. Cancellation is still allowed.",
            locale,
        ),
        "credit_limit": money_payload(credit_limit, account.currency),
    }


class TransactionCancellationService:
    """Canonical soft-cancellation flow shared by Mini App and bot internal API."""

    def __init__(self, user: TelegramUser) -> None:
        self.user = user
        self.locale = locale_for_user(user)

    def _transaction(self, transaction_id: int, *, lock: bool = False) -> Transaction | None:
        queryset = (
            _scoped_all_transactions_qs(self.user)
            .filter(id=transaction_id)
            .select_related("account", "category")
        )
        if lock:
            queryset = queryset.select_for_update(of=("self",))
        return queryset.first()

    def _validate_transaction(self, transaction: Transaction) -> None:
        if not _can_cancel_transaction(self.user, transaction):
            raise MiniAppHistoryError(
                "history_void_forbidden",
                mt("Ви не можете скасувати цю транзакцію.", "You cannot cancel this transaction.", self.locale),
                status=403,
            )
        if str(transaction.flow_kind or "") != "normal" or str(transaction.type or "") not in {"income", "expense"}:
            raise MiniAppHistoryError(
                "history_void_unsupported",
                mt(
                    "Скасувати можна лише звичайний дохід або витрату.",
                    "Only a regular income or expense can be cancelled.",
                    self.locale,
                ),
                status=409,
            )
        if transaction.account_id is None:
            raise MiniAppHistoryError(
                "history_account_missing",
                mt("Рахунок транзакції недоступний.", "The transaction account is unavailable.", self.locale),
                status=409,
            )

    def _locked_account(self, transaction: Transaction) -> Account:
        account = (
            _scoped_accounts_qs(self.user)
            .filter(id=transaction.account_id)
            .select_for_update()
            .first()
        )
        if account is None:
            raise MiniAppHistoryError(
                "history_account_missing",
                mt("Рахунок транзакції недоступний.", "The transaction account is unavailable.", self.locale),
                status=409,
            )
        return account

    def build_draft(self, payload: dict[str, object], *, draft_id: str) -> dict[str, object]:
        try:
            transaction_id = int(str(payload.get("transaction_id") or "").strip())
        except (TypeError, ValueError) as exc:
            raise MiniAppHistoryError(
                "history_transaction_missing",
                mt("Операцію не знайдено.", "Transaction was not found.", self.locale),
                status=404,
            ) from exc
        transaction = self._transaction(transaction_id)
        if transaction is None:
            raise MiniAppHistoryError(
                "history_transaction_missing",
                mt("Операцію не знайдено.", "Transaction was not found.", self.locale),
                status=404,
            )
        self._validate_transaction(transaction)
        if transaction.deleted_at is not None or bool(transaction.is_deleted):
            raise MiniAppHistoryError(
                "already_cancelled",
                mt("Цю транзакцію вже скасовано.", "This transaction has already been cancelled.", self.locale),
                status=409,
            )
        account = _scoped_accounts_qs(self.user).filter(id=transaction.account_id).first()
        if account is None:
            raise MiniAppHistoryError(
                "history_account_missing",
                mt("Рахунок транзакції недоступний.", "The transaction account is unavailable.", self.locale),
                status=409,
            )
        current_balance = _quantize(account.balance)
        balance_after = _quantize(
            current_balance - _balance_delta(str(transaction.type), _quantize(transaction.amount))
        )
        warning = _credit_limit_warning(account, balance_after, locale=self.locale)
        return {
            "draft_id": draft_id,
            "transaction_id": transaction_id,
            "original_signature": _transaction_signature(transaction),
            "transaction": _history_item(transaction, locale=self.locale, voidable=True),
            "current_balance": money_payload(current_balance, account.currency),
            "balance_after": money_payload(balance_after, account.currency),
            "warning": warning,
            "confirmation": {
                "title": mt("Скасувати транзакцію?", "Cancel transaction?", self.locale),
                "explanation": mt(
                    "Запис зникне з історії та звітів, а його вплив на баланс буде повністю повернуто.",
                    "The record will disappear from history and reports, and its balance effect will be fully reversed.",
                    self.locale,
                ),
            },
        }

    def _clear_expense_day_status_if_needed(self, transaction: Transaction) -> None:
        if str(transaction.type or "") != "expense" or not transaction.date:
            return
        if "daily_expense_day_statuses" not in set(connection.introspection.table_names()):
            return
        author_id = _transaction_author_id(transaction)
        remaining = Transaction.objects.filter(
            date=transaction.date,
            type="expense",
            flow_kind="normal",
        ).filter(
            Q(created_by_user_id=author_id)
            | Q(created_by_user_id__isnull=True, tg_user_id=author_id)
        )
        if transaction.family_id is None:
            remaining = remaining.filter(family_id__isnull=True)
        else:
            remaining = remaining.filter(family_id=transaction.family_id)
        if remaining.exists():
            return
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE daily_expense_day_statuses
                SET expense_recorded_at = NULL, updated_at = %s
                WHERE tg_user_id = %s AND day = %s
                """,
                [timezone.now(), author_id, transaction.date],
            )

    def commit_draft(self, draft: dict[str, object]) -> dict[str, object]:
        try:
            transaction_id = int(draft.get("transaction_id") or 0)
        except (TypeError, ValueError) as exc:
            raise MiniAppHistoryError(
                "history_transaction_missing",
                mt("Операцію не знайдено.", "Transaction was not found.", self.locale),
                status=404,
            ) from exc

        with db_transaction.atomic():
            transaction = self._transaction(transaction_id, lock=True)
            if transaction is None:
                raise MiniAppHistoryError(
                    "history_transaction_missing",
                    mt("Операцію не знайдено.", "Transaction was not found.", self.locale),
                    status=404,
                )
            self._validate_transaction(transaction)
            account = self._locked_account(transaction)
            if transaction.deleted_at is not None or bool(transaction.is_deleted):
                return {
                    "status": "already_cancelled",
                    "transaction_id": transaction_id,
                    "account": _account_payload(account, locale=self.locale),
                    "new_balance": money_payload(account.balance, account.currency),
                }
            if str(draft.get("original_signature") or "") != _transaction_signature(transaction):
                raise MiniAppHistoryError(
                    "history_transaction_changed",
                    mt(
                        "Транзакція змінилася. Перегляньте її ще раз перед скасуванням.",
                        "The transaction changed. Review it again before cancelling.",
                        self.locale,
                    ),
                    status=409,
                )

            current_balance = _quantize(account.balance)
            new_balance = _quantize(
                current_balance - _balance_delta(str(transaction.type), _quantize(transaction.amount))
            )
            current_type = _normalize_account_type(account.account_type)
            non_negative_type = _default_non_negative_account_type(
                account.non_negative_account_type or current_type or "main"
            )
            next_type = _resolve_runtime_account_type(
                balance=new_balance,
                non_negative_account_type=non_negative_type,
                current_account_type=current_type,
            )
            account.balance = new_balance
            account.account_type = next_type
            account.non_negative_account_type = non_negative_type
            account.save(update_fields=["balance", "account_type", "non_negative_account_type", "updated_at"])

            transaction.is_deleted = True
            transaction.deleted_at = timezone.now()
            transaction.deleted_by_user_id = self.user.tg_user_id
            transaction.save(update_fields=["is_deleted", "deleted_at", "deleted_by_user_id"])
            self._clear_expense_day_status_if_needed(transaction)

        status = "cancelled"
        if current_type != "credit" and next_type == "credit":
            status = "account_switched_to_credit"
        elif current_type == "credit" and next_type != "credit":
            status = "account_restored_from_credit"
        return {
            "status": status,
            "transaction_id": transaction_id,
            "account": _account_payload(account, locale=self.locale),
            "new_balance": money_payload(new_balance, account.currency),
            "warning": _credit_limit_warning(account, new_balance, locale=self.locale),
        }


def build_recent_cancellable_transactions(user: TelegramUser, *, limit: int = 10) -> dict[str, object]:
    service = TransactionCancellationService(user)
    scope = _resolve_finance_scope(user)
    queryset = _scoped_transactions_qs(user).filter(
        flow_kind="normal",
        type__in=("income", "expense"),
        account_id__isnull=False,
    )
    if scope.is_family and str(scope.role or "member").lower() != "owner":
        queryset = queryset.filter(
            Q(created_by_user_id=user.tg_user_id)
            | Q(created_by_user_id__isnull=True, tg_user_id=user.tg_user_id)
        )
    items = queryset.select_related("account", "category").order_by("-date", "-created_at", "-id")[: max(1, min(10, int(limit)))]
    return {
        "items": [_history_item(item, locale=service.locale, voidable=True) for item in items],
        "scope": {"type": scope.type, "role": scope.role},
    }


_HISTORY_CURSOR_SALT = "miniapp.history.cursor.v1"


def _history_cursor_context(user: TelegramUser, *, date_from: date, date_to: date) -> dict[str, object]:
    scope = _resolve_finance_scope(user)
    return {
        "actor_user_id": int(user.tg_user_id),
        "scope_type": scope.type,
        "family_id": int(scope.family_id) if scope.family_id is not None else None,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
    }


def _encode_history_cursor(
    transaction: Transaction,
    *,
    context: dict[str, object],
) -> str:
    return signing.dumps(
        {
            "date": transaction.date.isoformat(),
            "created_at": transaction.created_at.isoformat(),
            "id": int(transaction.id),
            "context": context,
        },
        salt=_HISTORY_CURSOR_SALT,
        compress=True,
    )


def _decode_history_cursor(
    value: str,
    *,
    expected_context: dict[str, object],
) -> tuple[date, datetime, int]:
    try:
        payload = signing.loads(value, salt=_HISTORY_CURSOR_SALT)
        cursor_date = date.fromisoformat(str(payload["date"]))
        cursor_created_at = datetime.fromisoformat(str(payload["created_at"]))
        cursor_id = int(payload["id"])
        if payload.get("context") != expected_context or cursor_created_at.tzinfo is None or cursor_id <= 0:
            raise ValueError("invalid cursor fields")
        return cursor_date, cursor_created_at, cursor_id
    except (signing.BadSignature, KeyError, TypeError, ValueError) as exc:
        raise MiniAppHistoryError("invalid_history_cursor", "The history page cursor is invalid.", status=400) from exc


def build_history_payload(
    user: TelegramUser,
    *,
    date_from,
    date_to,
    limit: int = 100,
    cursor: str | None = None,
) -> dict[str, object]:
    locale = locale_for_user(user)
    page_size = max(1, min(int(limit), 200))
    cursor_context = _history_cursor_context(user, date_from=date_from, date_to=date_to)
    queryset = (
        _scoped_transactions_qs(user)
        .filter(flow_kind="normal", date__gte=date_from, date__lte=date_to)
        .select_related("account", "category")
        .order_by("-date", "-created_at", "-id")
    )
    if cursor:
        cursor_date, cursor_created_at, cursor_id = _decode_history_cursor(
            cursor,
            expected_context=cursor_context,
        )
        queryset = queryset.filter(
            Q(date__lt=cursor_date)
            | Q(date=cursor_date, created_at__lt=cursor_created_at)
            | Q(date=cursor_date, created_at=cursor_created_at, id__lt=cursor_id)
        )
    transactions = list(queryset[: page_size + 1])
    has_more = len(transactions) > page_size
    transactions = transactions[:page_size]
    category_groups: dict[str, list[dict[str, object]]] = {"expense": [], "income": []}
    for category in _scoped_categories_qs(user).filter(is_active=True, deleted_at__isnull=True).order_by("sort_order", "name", "id")[:200]:
        kind = _category_kind(category)
        if kind in category_groups:
            category_groups[kind].append(_category_payload(category))
    return {
        "items": [
            _history_item(item, locale=locale, voidable=_can_cancel_transaction(user, item))
            for item in transactions
        ],
        "accounts": [
            _account_payload(account, locale=locale)
            for account in _scoped_accounts_qs(user).filter(is_active=True).order_by("label", "id")[:100]
        ],
        "categories": category_groups,
        "has_more": has_more,
        "next_cursor": (
            _encode_history_cursor(transactions[-1], context=cursor_context)
            if has_more and transactions
            else None
        ),
    }


def _balance_delta(kind: str, amount: Decimal) -> Decimal:
    return amount if kind == "income" else -amount


def _projected_balances(
    user: TelegramUser,
    original: Transaction,
    replacement: dict[str, object],
    *,
    locale: str,
) -> list[dict[str, object]]:
    if original.account_id is None:
        raise MiniAppHistoryError("history_account_missing", "The original account is not available.", status=409)
    replacement_account = replacement.get("account") if isinstance(replacement.get("account"), dict) else {}
    replacement_account_id = _parse_positive_int(
        replacement.get("account_id") or replacement_account.get("id"),
        code="account_missing",
        message="Account is required.",
    )
    account_ids = {int(original.account_id), replacement_account_id}
    accounts = {
        int(account.id): account
        for account in _scoped_accounts_qs(user).filter(id__in=account_ids)
    }
    original_account = accounts.get(int(original.account_id))
    target_account = accounts.get(replacement_account_id)
    if original_account is None or target_account is None:
        raise MiniAppHistoryError("history_account_missing", "One of the accounts is not available.", status=409)
    changes = {account_id: _quantize(account.balance) for account_id, account in accounts.items()}
    changes[int(original_account.id)] = _quantize(changes[int(original_account.id)] - _balance_delta(str(original.type), _quantize(original.amount)))
    amount = _parse_transaction_amount(replacement.get("amount_value"))
    kind = str(replacement.get("kind") or "").strip().lower()
    changes[int(target_account.id)] = _quantize(changes[int(target_account.id)] + _balance_delta(kind, amount))
    return [
        {
            "account": _account_payload(account, locale=locale),
            "balance_after": money_payload(changes[int(account.id)], account.currency),
        }
        for account in accounts.values()
    ]


def build_history_edit_draft(user: TelegramUser, payload: dict[str, object], *, draft_id: str) -> dict[str, object]:
    try:
        transaction_id = int(str(payload.get("transaction_id") or "").strip())
    except (TypeError, ValueError) as exc:
        raise MiniAppHistoryError("history_transaction_missing", "Choose a saved transaction.", status=404) from exc
    original = _normal_transaction(user, transaction_id)
    if original is None:
        raise MiniAppHistoryError("history_transaction_missing", "The transaction is no longer available for editing.", status=404)
    if not _can_cancel_transaction(user, original):
        raise MiniAppHistoryError("history_edit_forbidden", "You cannot edit this transaction.", status=403)
    if original.account_id is None or original.category_id is None:
        raise MiniAppHistoryError("history_transaction_unsupported", "Only regular income and expense transactions can be edited.", status=409)

    replacement_payload = {
        "kind": payload.get("kind"),
        "amount": payload.get("amount"),
        "account_id": payload.get("account_id"),
        "category_id": payload.get("category_id"),
        "transaction_date": payload.get("transaction_date"),
        "comment": payload.get("comment"),
    }
    from miniapp.services import build_transaction_draft

    replacement = build_transaction_draft(user, replacement_payload, draft_id=draft_id)
    locale = locale_for_user(user)
    balance_changes = _projected_balances(user, original, replacement, locale=locale)
    return {
        "draft_id": draft_id,
        "transaction_id": transaction_id,
        "original_signature": _transaction_signature(original),
        "replacement": replacement,
        "balance_changes": balance_changes,
        "confirmation": {
            "title": mt("Підтвердьте зміни операції", "Confirm transaction changes", locale),
            "summary": replacement["confirmation"]["summary"],
        },
    }


def _validate_balance(account: Account, projected_balance: Decimal) -> None:
    credit_limit = _quantize(account.credit_limit) if account.credit_limit is not None else None
    if projected_balance < ZERO and credit_limit is not None and abs(projected_balance) > credit_limit:
        raise MiniAppTransactionError(
            "credit_limit_exceeded",
            "This edit exceeds the account credit limit.",
            extra={
                "projected_balance": money_payload(projected_balance, account.currency),
                "credit_limit": money_payload(credit_limit, account.currency),
            },
        )


def commit_history_edit_draft(user: TelegramUser, draft: dict[str, object]) -> dict[str, object]:
    try:
        transaction_id = int(draft.get("transaction_id") or 0)
    except (TypeError, ValueError) as exc:
        raise MiniAppHistoryError("history_transaction_missing", "The transaction is no longer available for editing.", status=404) from exc
    replacement = draft.get("replacement") if isinstance(draft.get("replacement"), dict) else {}
    if not replacement:
        raise MiniAppHistoryError("history_draft_invalid", "The transaction draft is invalid.")

    kind = str(replacement.get("kind") or "").strip().lower()
    if kind not in {"income", "expense"}:
        raise MiniAppHistoryError("history_draft_invalid", "Transaction type must be income or expense.")
    amount = _parse_transaction_amount(replacement.get("amount_value"))
    account_id = _parse_positive_int(
        replacement.get("account_id") or (replacement.get("account") or {}).get("id"),
        code="account_missing",
        message="Account is required.",
    )
    category_id = _parse_positive_int(
        replacement.get("category_id") or (replacement.get("category") or {}).get("id"),
        code="category_missing",
        message="Category is required.",
    )
    transaction_date = _parse_transaction_date(replacement.get("transaction_date"))
    comment = str(replacement.get("comment") or "").strip()[:500] or None

    with db_transaction.atomic():
        original = _normal_transaction(user, transaction_id, lock=True)
        if original is None or original.account_id is None or original.category_id is None:
            raise MiniAppHistoryError("history_transaction_missing", "The transaction is no longer available for editing.", status=404)
        if not _can_cancel_transaction(user, original):
            raise MiniAppHistoryError("history_edit_forbidden", "You cannot edit this transaction.", status=403)
        if str(draft.get("original_signature") or "") != _transaction_signature(original):
            raise MiniAppHistoryError("history_transaction_changed", "The transaction changed. Review it again before saving.", status=409)

        account_ids = sorted({int(original.account_id), account_id})
        locked_accounts = list(_scoped_accounts_qs(user).filter(id__in=account_ids).select_for_update().order_by("id"))
        accounts = {int(account.id): account for account in locked_accounts}
        original_account = accounts.get(int(original.account_id))
        target_account = accounts.get(account_id)
        if original_account is None or target_account is None or not target_account.is_active:
            raise MiniAppHistoryError("history_account_missing", "One of the accounts is not available.", status=409)
        category = _active_scoped_category(user, category_id, kind)
        target_currency = _normalize_currency(target_account.currency)

        balances = {int(account.id): _quantize(account.balance) for account in locked_accounts}
        balances[int(original_account.id)] = _quantize(
            balances[int(original_account.id)] - _balance_delta(str(original.type), _quantize(original.amount))
        )
        balances[int(target_account.id)] = _quantize(balances[int(target_account.id)] + _balance_delta(kind, amount))
        for account in locked_accounts:
            _validate_balance(account, balances[int(account.id)])

        for account in locked_accounts:
            next_balance = balances[int(account.id)]
            current_type = _normalize_account_type(account.account_type)
            non_negative_type = _default_non_negative_account_type(account.non_negative_account_type or current_type or "main")
            account.balance = next_balance
            account.account_type = _resolve_runtime_account_type(
                balance=next_balance,
                non_negative_account_type=non_negative_type,
                current_account_type=current_type,
            )
            account.non_negative_account_type = non_negative_type
            account.save(update_fields=["balance", "account_type", "non_negative_account_type", "updated_at"])

        original.date = transaction_date
        original.type = kind
        original.amount = amount
        original.currency = target_currency
        original.comment = comment
        original.account = target_account
        original.category = category
        original.category_name_snapshot = category.name
        original.save(
            update_fields=[
                "date",
                "type",
                "amount",
                "currency",
                "comment",
                "account",
                "category",
                "category_name_snapshot",
            ]
        )

    locale = locale_for_user(user)
    return {
        "status": "completed",
        "transaction": _history_item(original, locale=locale),
        "accounts": [_account_payload(account, locale=locale) for account in locked_accounts],
    }
