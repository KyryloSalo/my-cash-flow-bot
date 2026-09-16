from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from django.db import connection, transaction as db_transaction
from django.utils import timezone

from accounts.models import Account
from transactions.models import Transaction
from users.models import TelegramUser

from miniapp.services import (
    MiniAppTransactionError,
    _normalize_currency,
    _resolve_finance_scope,
    build_transfer_draft,
    commit_transfer_draft,
    locale_for_user,
    money_payload,
    mt,
)


class MiniAppSavingTaskError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def _task_table_available() -> bool:
    return "pending_saving_tasks" in connection.introspection.table_names()


def _require_personal_saving_tasks(user: TelegramUser) -> None:
    if not _task_table_available():
        raise MiniAppSavingTaskError("saving_tasks_unavailable", "Planned savings are temporarily unavailable.", status=503)
    if _resolve_finance_scope(user).is_family:
        raise MiniAppSavingTaskError(
            "saving_tasks_personal_only",
            "Planned savings are available only for a personal budget.",
            status=409,
        )


def _row_dict(cursor) -> dict[str, object] | None:
    row = cursor.fetchone()
    if row is None:
        return None
    return dict(zip((column[0] for column in cursor.description), row, strict=True))


def _rows_dict(cursor) -> list[dict[str, object]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _task_row(user: TelegramUser, task_id: int, *, for_update: bool = False) -> dict[str, object] | None:
    suffix = " FOR UPDATE OF pst" if for_update else ""
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT pst.id, pst.tg_user_id, pst.source_account_id, pst.target_account_id,
                   pst.amount, pst.currency, pst.percent_from_income, pst.income_amount,
                   pst.status, pst.remind_at, pst.reminded_at, pst.created_at,
                   pst.completed_at, pst.cancelled_at,
                   src.label AS source_label, src.currency AS source_currency,
                   dst.label AS target_label, dst.currency AS target_currency
            FROM pending_saving_tasks pst
            JOIN accounts src ON src.id = pst.source_account_id
            JOIN accounts dst ON dst.id = pst.target_account_id
            WHERE pst.tg_user_id = %s AND pst.id = %s{suffix}
            """,
            [int(user.tg_user_id), int(task_id)],
        )
        return _row_dict(cursor)


def _active_personal_account(user: TelegramUser, account_id: int, *, lock: bool = False) -> Account | None:
    queryset = Account.objects.filter(
        id=int(account_id),
        tg_user_id=int(user.tg_user_id),
        family_id__isnull=True,
        is_active=True,
    )
    if lock:
        queryset = queryset.select_for_update()
    return queryset.first()


def _parse_task_id(value: object) -> int:
    try:
        task_id = int(str(value or "").strip())
    except (TypeError, ValueError) as exc:
        raise MiniAppSavingTaskError("saving_task_missing", "Choose a planned saving.", status=404) from exc
    if task_id <= 0:
        raise MiniAppSavingTaskError("saving_task_missing", "Choose a planned saving.", status=404)
    return task_id


def _parse_amount(value: object) -> Decimal:
    raw = str(value or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise MiniAppSavingTaskError("saving_amount_invalid", "Amount must be a positive number.") from exc
    amount = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if amount <= 0:
        raise MiniAppSavingTaskError("saving_amount_invalid", "Amount must be a positive number.")
    return amount


def _parse_future_reminder(value: object) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise MiniAppSavingTaskError("saving_reminder_invalid", "Choose a future reminder time.")
    try:
        reminder = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MiniAppSavingTaskError("saving_reminder_invalid", "Choose a valid reminder time.") from exc
    if timezone.is_naive(reminder):
        reminder = timezone.make_aware(reminder, timezone.get_current_timezone())
    if reminder <= timezone.now():
        raise MiniAppSavingTaskError("saving_reminder_invalid", "Choose a future reminder time.")
    return reminder


def _serialize_datetime(value: object) -> str | None:
    return value.isoformat() if value is not None else None


def _serialize_task(row: dict[str, object], *, locale: str) -> dict[str, object]:
    currency = _normalize_currency(str(row.get("currency") or row.get("source_currency") or "UAH"))
    return {
        "id": int(row["id"]),
        "amount": money_payload(row.get("amount") or 0, currency),
        "currency": currency,
        "status": str(row.get("status") or "pending"),
        "source": {
            "id": int(row["source_account_id"]),
            "label": str(row.get("source_label") or ""),
            "currency": _normalize_currency(str(row.get("source_currency") or currency)),
        },
        "target": {
            "id": int(row["target_account_id"]),
            "label": str(row.get("target_label") or ""),
            "currency": _normalize_currency(str(row.get("target_currency") or currency)),
        },
        "percent_from_income": f"{Decimal(str(row['percent_from_income'])):.2f}" if row.get("percent_from_income") is not None else None,
        "income_amount": money_payload(row["income_amount"], currency) if row.get("income_amount") is not None else None,
        "remind_at": _serialize_datetime(row.get("remind_at")),
        "created_at": _serialize_datetime(row.get("created_at")),
        "labels": {
            "pending": mt("Очікує підтвердження", "Pending confirmation", locale),
        },
    }


def build_saving_tasks_payload(user: TelegramUser) -> dict[str, object]:
    _require_personal_saving_tasks(user)
    locale = locale_for_user(user)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT pst.id, pst.tg_user_id, pst.source_account_id, pst.target_account_id,
                   pst.amount, pst.currency, pst.percent_from_income, pst.income_amount,
                   pst.status, pst.remind_at, pst.reminded_at, pst.created_at,
                   pst.completed_at, pst.cancelled_at,
                   src.label AS source_label, src.currency AS source_currency,
                   dst.label AS target_label, dst.currency AS target_currency
            FROM pending_saving_tasks pst
            JOIN accounts src ON src.id = pst.source_account_id
            JOIN accounts dst ON dst.id = pst.target_account_id
            WHERE pst.tg_user_id = %s AND pst.status = 'pending'
            ORDER BY pst.created_at DESC, pst.id DESC
            """,
            [int(user.tg_user_id)],
        )
        tasks = _rows_dict(cursor)
    target_accounts = list(
        Account.objects.filter(
            tg_user_id=int(user.tg_user_id),
            family_id__isnull=True,
            is_active=True,
        )
        .order_by("label", "id")
        .values("id", "label", "currency", "account_type")
    )
    return {
        "available": True,
        "tasks": [_serialize_task(task, locale=locale) for task in tasks],
        "target_accounts": [
            {
                "id": int(account["id"]),
                "label": str(account.get("label") or ""),
                "currency": _normalize_currency(str(account.get("currency") or "UAH")),
                "account_type": str(account.get("account_type") or ""),
            }
            for account in target_accounts
        ],
    }


def _saving_task_snapshot(task: dict[str, object]) -> dict[str, object]:
    return {
        "version": 1,
        "id": int(task["id"]),
        "tg_user_id": int(task["tg_user_id"]),
        "source_account_id": int(task["source_account_id"]),
        "target_account_id": int(task["target_account_id"]),
        "amount": f"{Decimal(str(task['amount'])):.2f}",
        "currency": _normalize_currency(str(task.get("currency") or "")),
        "source_currency": _normalize_currency(str(task.get("source_currency") or "")),
        "target_currency": _normalize_currency(str(task.get("target_currency") or "")),
        "status": str(task.get("status") or ""),
    }


def build_saving_task_draft(user: TelegramUser, payload: dict[str, object], *, draft_id: str) -> dict[str, object]:
    _require_personal_saving_tasks(user)
    locale = locale_for_user(user)
    task_id = _parse_task_id(payload.get("task_id"))
    task = _task_row(user, task_id)
    if task is None:
        raise MiniAppSavingTaskError("saving_task_missing", "The planned saving is no longer available.", status=404)
    if str(task.get("status") or "") != "pending":
        raise MiniAppSavingTaskError("saving_task_not_pending", "The planned saving is already handled.", status=409)
    source = _active_personal_account(user, int(task["source_account_id"]))
    if source is None:
        raise MiniAppSavingTaskError("saving_source_missing", "The source account is no longer available.", status=409)

    action = str(payload.get("action") or "").strip().lower()
    snapshot = _saving_task_snapshot(task)
    if action == "confirm":
        transfer = build_transfer_draft(
            user,
            {
                "source_account_id": int(task["source_account_id"]),
                "target_account_id": int(task["target_account_id"]),
                "amount": str(task["amount"]),
                "fx_rate": payload.get("fx_rate"),
                "transaction_date": payload.get("transaction_date"),
                "comment": "saving transfer",
            },
            draft_id=draft_id,
        )
        return {
            "draft_id": draft_id,
            "action": action,
            "task_id": task_id,
            "task_snapshot": snapshot,
            "transfer": transfer,
            "confirmation": transfer["confirmation"],
        }

    if action == "update":
        amount = _parse_amount(payload.get("amount"))
        try:
            target_id = int(str(payload.get("target_account_id") or "").strip())
        except (TypeError, ValueError) as exc:
            raise MiniAppSavingTaskError("saving_target_missing", "Choose a target account.") from exc
        target = _active_personal_account(user, target_id)
        if target is None:
            raise MiniAppSavingTaskError("saving_target_missing", "Choose an active target account.")
        if int(source.id) == int(target.id):
            raise MiniAppSavingTaskError("same_account", "Choose two different accounts.")
        return {
            "draft_id": draft_id,
            "action": action,
            "task_id": task_id,
            "task_snapshot": snapshot,
            "amount_value": f"{amount:.2f}",
            "target_account_id": int(target.id),
            "confirmation": {
                "title": mt("Підтвердьте зміни накопичення", "Confirm planned saving changes", locale),
                "summary": [
                    money_payload(amount, source.currency)["display"],
                    f"{source.label} → {target.label}",
                ],
            },
        }

    if action == "remind":
        reminder = _parse_future_reminder(payload.get("remind_at"))
        return {
            "draft_id": draft_id,
            "action": action,
            "task_id": task_id,
            "task_snapshot": snapshot,
            "remind_at": reminder.isoformat(),
            "confirmation": {
                "title": mt("Підтвердьте нагадування", "Confirm reminder", locale),
                "summary": [reminder.astimezone(timezone.get_current_timezone()).strftime("%d.%m.%Y %H:%M")],
            },
        }

    if action == "cancel":
        return {
            "draft_id": draft_id,
            "action": action,
            "task_id": task_id,
            "task_snapshot": snapshot,
            "confirmation": {
                "title": mt("Скасувати накопичення", "Cancel planned saving", locale),
                "summary": [str(task.get("source_label") or ""), str(task.get("target_label") or "")],
            },
        }

    raise MiniAppSavingTaskError("saving_action_invalid", "Choose an available planned saving action.")


def commit_saving_task_draft(user: TelegramUser, draft: dict[str, object]) -> dict[str, object]:
    _require_personal_saving_tasks(user)
    task_id = _parse_task_id(draft.get("task_id"))
    action = str(draft.get("action") or "").strip().lower()
    if action not in {"confirm", "update", "remind", "cancel"}:
        raise MiniAppSavingTaskError("saving_action_invalid", "Choose an available planned saving action.")

    with db_transaction.atomic():
        task = _task_row(user, task_id, for_update=True)
        if task is None:
            raise MiniAppSavingTaskError("saving_task_missing", "The planned saving is no longer available.", status=404)
        if str(task.get("status") or "") != "pending":
            raise MiniAppSavingTaskError("saving_task_not_pending", "The planned saving is already handled.", status=409)

        if draft.get("task_snapshot") != _saving_task_snapshot(task):
            raise MiniAppSavingTaskError("saving_task_changed", "The planned saving changed. Review it again before confirming.", status=409)

        if action == "confirm":
            # Lock in the transfer service's account order, then refresh the
            # joined currencies: the task lock alone does not protect accounts.
            accounts = {
                account_id: _active_personal_account(user, account_id, lock=True)
                for account_id in sorted({int(task["source_account_id"]), int(task["target_account_id"])})
            }
            source = accounts[int(task["source_account_id"])]
            target = accounts[int(task["target_account_id"])]
            if source is None or target is None:
                raise MiniAppSavingTaskError("saving_task_changed", "A saving account is no longer available. Review the task again.", status=409)
            task["source_currency"] = source.currency
            task["target_currency"] = target.currency
            if draft.get("task_snapshot") != _saving_task_snapshot(task):
                raise MiniAppSavingTaskError("saving_task_changed", "The planned saving changed. Review it again before confirming.", status=409)
            transfer_draft = draft.get("transfer")
            if not isinstance(transfer_draft, dict):
                raise MiniAppSavingTaskError("saving_task_changed", "Review the saving transfer again before confirming.", status=409)
            transfer = commit_transfer_draft(user, transfer_draft)
            transaction_id = int((transfer.get("transfer") or {}).get("id") or 0)
            if transaction_id <= 0:
                raise MiniAppSavingTaskError("saving_transfer_failed", "The saving transfer could not be completed.", status=409)
            Transaction.objects.filter(id=transaction_id).update(transfer_subtype="savings_transfer")
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE pending_saving_tasks
                    SET status = 'completed', completed_at = now(), remind_at = NULL
                    WHERE tg_user_id = %s AND id = %s AND status = 'pending'
                    """,
                    [int(user.tg_user_id), task_id],
                )
            return {"status": "completed", "transfer": transfer}

        if action == "update":
            amount = _parse_amount(draft.get("amount_value"))
            try:
                target_id = int(draft.get("target_account_id") or 0)
            except (TypeError, ValueError) as exc:
                raise MiniAppSavingTaskError("saving_target_missing", "Choose a target account.") from exc
            source = _active_personal_account(user, int(task["source_account_id"]), lock=True)
            target = _active_personal_account(user, target_id, lock=True)
            if source is None or target is None:
                raise MiniAppSavingTaskError("saving_target_missing", "Choose an active source and target account.", status=409)
            if int(source.id) == int(target.id):
                raise MiniAppSavingTaskError("same_account", "Choose two different accounts.")
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE pending_saving_tasks
                    SET amount = %s, target_account_id = %s
                    WHERE tg_user_id = %s AND id = %s AND status = 'pending'
                    """,
                    [amount, int(target.id), int(user.tg_user_id), task_id],
                )
            return {"status": "updated"}

        if action == "remind":
            reminder = _parse_future_reminder(draft.get("remind_at"))
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE pending_saving_tasks
                    SET remind_at = %s, reminded_at = NULL
                    WHERE tg_user_id = %s AND id = %s AND status = 'pending'
                    """,
                    [reminder, int(user.tg_user_id), task_id],
                )
            return {"status": "reminder_set", "remind_at": reminder.isoformat()}

        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pending_saving_tasks
                SET status = 'cancelled', cancelled_at = now(), remind_at = NULL
                WHERE tg_user_id = %s AND id = %s AND status = 'pending'
                """,
                [int(user.tg_user_id), task_id],
            )
        return {"status": "cancelled"}
