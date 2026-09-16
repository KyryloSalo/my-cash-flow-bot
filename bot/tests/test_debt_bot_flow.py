from __future__ import annotations

import os
import re
import sys
import types
import unittest
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import unquote
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Pool = object
    asyncpg_stub.Record = object
    asyncpg_stub.create_pool = AsyncMock()
    sys.modules["asyncpg"] = asyncpg_stub

if "telegram" not in sys.modules:
    telegram_stub = types.ModuleType("telegram")

    class _InlineKeyboardButton:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.text = args[0] if args else kwargs.get("text", "")
            self.callback_data = kwargs.get("callback_data")

    class _InlineKeyboardMarkup:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.inline_keyboard = args[0] if args else kwargs.get("inline_keyboard", [])

    class _Update:
        pass

    telegram_stub.Update = _Update
    telegram_stub.InlineKeyboardButton = _InlineKeyboardButton
    telegram_stub.InlineKeyboardMarkup = _InlineKeyboardMarkup
    sys.modules["telegram"] = telegram_stub

    constants_stub = types.ModuleType("telegram.constants")
    constants_stub.ChatAction = types.SimpleNamespace(TYPING="typing")
    constants_stub.ParseMode = types.SimpleNamespace(HTML="HTML")
    sys.modules["telegram.constants"] = constants_stub

    ext_stub = types.ModuleType("telegram.ext")

    class _Stub:
        pass

    _Stub.END = -1

    ext_stub.Application = _Stub
    ext_stub.CallbackQueryHandler = _Stub
    ext_stub.CommandHandler = _Stub
    ext_stub.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
    ext_stub.Defaults = _Stub
    ext_stub.ConversationHandler = _Stub
    ext_stub.MessageHandler = _Stub
    ext_stub.filters = types.SimpleNamespace(TEXT=object(), COMMAND=object(), VOICE=object())
    sys.modules["telegram.ext"] = ext_stub

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")

    class _AsyncClient:
        pass

    httpx_stub.AsyncClient = _AsyncClient
    sys.modules["httpx"] = httpx_stub

import bot_main  # noqa: E402
from tests.financial_fixtures import confirmation_from_reply, prepare_confirmation


class DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyMessage:
    def __init__(self, text: str = "", voice_duration: int | None = None) -> None:
        self.text = text
        self.voice = SimpleNamespace(duration=voice_duration, get_file=AsyncMock())
        self.replies: list[dict] = []
        self.chat = SimpleNamespace(send_action=AsyncMock())

    async def reply_text(self, text: str, reply_markup=None, **kwargs) -> None:
        self.replies.append({"text": text, "reply_markup": reply_markup, "kwargs": kwargs})


class DummyUpdate:
    def __init__(self, message: DummyMessage) -> None:
        self.message = message
        self.effective_user = SimpleNamespace(id=77, username="tester", first_name="Tester", last_name="")


class DebtTextConn:
    def __init__(self) -> None:
        self.user = {"tg_user_id": 77, "base_currency": "UAH", "start_date": date(2026, 1, 1)}
        self.accounts = [
            {"id": 1, "tg_user_id": 77, "label": "Main", "currency": "UAH", "account_type": "card", "starting_balance": 0, "balance": 0, "is_active": True, "created_at": date(2026, 1, 1), "updated_at": date(2026, 1, 1)}
        ]
        self.debts: dict[int, dict] = {}
        self.execute_calls: list[tuple[str, tuple]] = []
        self.billing_state = {
            "profile_exists": True,
            "has_card": True,
            "masked_pan": "444455******1111",
            "profile_status": "active",
            "auto_renew_enabled": True,
            "last_charge_status": "",
            "last_failure_reason": "",
            "last_action_url": "",
            "subscription_status": "trial",
            "expires_at": datetime(2030, 1, 1, tzinfo=UTC),
            "next_charge_at": datetime(2030, 1, 1, tzinfo=UTC),
            "grace_expires_at": None,
            "trial_days": 30,
            "access_mode": "full",
        }
        self.access_state = {
            "access_scope": "personal_full",
            "access_source": "billing",
            "pending_start_payload": "",
        }

    def transaction(self) -> DummyTx:
        return DummyTx()

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM family_members" in normalized and "JOIN families" in normalized:
            return None
        if "FROM users" in normalized:
            return dict(self.user)
        if "FROM billing_profiles" in normalized and "WHERE telegram_user_id=$1" in normalized:
            return {
                "status": self.billing_state["profile_status"],
                "masked_pan": self.billing_state["masked_pan"],
                "auto_renew_enabled": self.billing_state["auto_renew_enabled"],
                "last_charge_status": self.billing_state["last_charge_status"],
                "last_failure_reason": self.billing_state["last_failure_reason"],
                "last_action_url": self.billing_state["last_action_url"],
                "has_card": bool(self.billing_state["has_card"]),
            }
        if "FROM subscriptions" in normalized and "WHERE telegram_user_id=$1" in normalized:
            return {
                "status": self.billing_state["subscription_status"],
                "expires_at": self.billing_state["expires_at"],
                "next_charge_at": self.billing_state["next_charge_at"],
                "grace_expires_at": self.billing_state["grace_expires_at"],
                "trial_days": self.billing_state["trial_days"],
            }
        if "FROM user_admin_states" in normalized and "SELECT access_scope, access_source, pending_start_payload" in normalized:
            return dict(self.access_state)
        if "FROM accounts" in normalized:
            return dict(self.accounts[0])
        if "FROM debts" in normalized and "WHERE tg_user_id=$1 AND id=$2" in normalized:
            debt = self.debts.get(int(args[1]))
            return dict(debt) if debt is not None else None
        return None

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT to_regclass($1) IS NOT NULL" in normalized:
            return str(args[0]) in {"billing_profiles", "subscriptions", "user_admin_states", "promo_offers"}
        if "SELECT 1 FROM users WHERE tg_user_id=$1" in normalized:
            return 1
        if "count(1)" in normalized and "FROM accounts" in normalized:
            return 1
        return None

    async def execute(self, query: str, *args) -> None:
        self.execute_calls.append((query, args))

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM debts" in normalized and "LEFT JOIN debt_payments" not in normalized:
            rows = [dict(row) for row in self.debts.values() if row.get("tg_user_id") == 77]
            if "direction=$2" in normalized and len(args) > 1:
                rows = [row for row in rows if row.get("direction") == args[1]]
            if "status = ANY($3)" in normalized and len(args) > 2:
                allowed = {str(value) for value in args[2]}
                rows = [row for row in rows if row.get("status") in allowed]
            elif "status = ANY($2)" in normalized and len(args) > 1:
                allowed = {str(value) for value in args[1]}
                rows = [row for row in rows if row.get("status") in allowed]
            if "remaining_amount > 0" in normalized:
                rows = [row for row in rows if Decimal(str(row.get("remaining_amount") or 0)) > 0]
            rows.sort(key=lambda row: (row["created_at"], row["id"]))
            return rows
        return []


def _make_callback_update(message: DummyMessage, data: str) -> DummyUpdate:
    callback_query = SimpleNamespace(data=data, message=message, answer=AsyncMock())
    update = DummyUpdate(message)
    update.callback_query = callback_query
    return update


def _keyboard_labels(reply_markup) -> list[str]:
    rows = getattr(reply_markup, "args", [None])[0] or []
    labels: list[str] = []
    for row in rows:
        for button in row:
            labels.append(str(button.args[0]))
    return labels


def _keyboard_urls(reply_markup) -> list[str]:
    rows = getattr(reply_markup, "args", [None])[0] or []
    urls: list[str] = []
    for row in rows:
        for button in row:
            url = button.kwargs.get("url")
            if url:
                urls.append(str(url))
    return urls


class DebtBotFlowTests(unittest.IsolatedAsyncioTestCase):
    def _populate_debt(
        self,
        conn: DebtTextConn,
        *,
        debt_id: int,
        counterparty_name: str,
        direction: str,
        remaining_amount: str,
        initial_amount: str = "1000.00",
        paid_amount: str = "0",
        status: str = "active",
        currency: str = "UAH",
    ) -> None:
        conn.debts[debt_id] = {
            "id": debt_id,
            "tg_user_id": 77,
            "counterparty_name": counterparty_name,
            "direction": direction,
            "initial_amount": Decimal(initial_amount),
            "paid_amount": Decimal(paid_amount),
            "remaining_amount": Decimal(remaining_amount),
            "currency": currency,
            "account_id": 1,
            "status": status,
            "due_date": None,
            "comment": None,
            "created_at": date(2026, 5, 1),
            "updated_at": date(2026, 5, 1),
            "closed_at": None,
        }

    async def test_start_debt_from_text_creates_draft_without_writing(self) -> None:
        conn = DebtTextConn()
        message = DummyMessage(text="Дав Саші 1000 грн у борг")
        update = DummyUpdate(message)
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        with patch.object(bot_main, "_pool", return_value=_Pool(conn)), patch.object(
            bot_main, "_user_ready", AsyncMock(return_value=True)
        ), patch.object(bot_main, "_get_user", AsyncMock(return_value=conn.user)):
            result = await bot_main._start_debt_from_text(update, context, message.text)

        self.assertTrue(result)
        self.assertEqual(len(conn.execute_calls), 0)
        self.assertIn("debt_flow", context.user_data)
        self.assertEqual(context.user_data["debt_flow"]["direction"], "receivable")
        self.assertEqual(context.user_data["debt_flow"]["step"], bot_main.DEBT_FLOW_STEP_CHOOSING_ACCOUNT)
        self.assertTrue(message.replies)

    async def test_debt_summary_receivable_is_available_in_repayment_in_flow(self) -> None:
        conn = DebtTextConn()
        self._populate_debt(
            conn,
            debt_id=1,
            counterparty_name="Олег",
            direction="receivable",
            initial_amount="1000.00",
            paid_amount="0",
            remaining_amount="1000.00",
            status="active",
        )
        message = DummyMessage()
        update = DummyUpdate(message)
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        with patch.object(bot_main, "_pool", return_value=_Pool(conn)), patch.object(
            bot_main, "_user_ready", AsyncMock(return_value=True)
        ), patch.object(bot_main, "_get_user", AsyncMock(return_value=conn.user)):
            summary = await bot_main._debts_report_text(conn, 77)
            await bot_main.debt_callback(_make_callback_update(message, "debt:repay"), context)
            await bot_main.debt_callback(_make_callback_update(message, "debt:repay:in"), context)

        self.assertIn("1 000", summary)
        self.assertTrue(message.replies)
        self.assertNotIn("Активних боргів для цього типу немає", message.replies[-1]["text"])
        labels = _keyboard_labels(message.replies[-1]["reply_markup"])
        self.assertTrue(any("Олег" in label for label in labels))
        self.assertTrue(any("1 000" in label for label in labels))

    async def test_payable_debt_appears_in_repayment_out_flow(self) -> None:
        conn = DebtTextConn()
        self._populate_debt(
            conn,
            debt_id=2,
            counterparty_name="Іра",
            direction="payable",
            initial_amount="1000.00",
            paid_amount="0",
            remaining_amount="1000.00",
            status="active",
        )
        message = DummyMessage()
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        with patch.object(bot_main, "_pool", return_value=_Pool(conn)), patch.object(
            bot_main, "_user_ready", AsyncMock(return_value=True)
        ), patch.object(bot_main, "_get_user", AsyncMock(return_value=conn.user)):
            await bot_main.debt_callback(_make_callback_update(message, "debt:repay"), context)
            await bot_main.debt_callback(_make_callback_update(message, "debt:repay:out"), context)

        self.assertTrue(message.replies)
        self.assertNotIn("Активних боргів для цього типу немає", message.replies[-1]["text"])
        labels = _keyboard_labels(message.replies[-1]["reply_markup"])
        self.assertTrue(any("Іра" in label for label in labels))
        self.assertTrue(any("1 000" in label for label in labels))

    async def test_repayment_in_flow_does_not_show_payable_debts(self) -> None:
        conn = DebtTextConn()
        self._populate_debt(
            conn,
            debt_id=3,
            counterparty_name="Іра",
            direction="payable",
            initial_amount="1000.00",
            paid_amount="0",
            remaining_amount="1000.00",
            status="active",
        )
        message = DummyMessage()
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        with patch.object(bot_main, "_pool", return_value=_Pool(conn)), patch.object(
            bot_main, "_user_ready", AsyncMock(return_value=True)
        ), patch.object(bot_main, "_get_user", AsyncMock(return_value=conn.user)):
            await bot_main.debt_callback(_make_callback_update(message, "debt:repay"), context)
            await bot_main.debt_callback(_make_callback_update(message, "debt:repay:in"), context)

        self.assertTrue(message.replies)
        self.assertIn("Активних боргів для цього типу немає", message.replies[-1]["text"])
        self.assertNotIn("Іра", message.replies[-1]["text"])

    async def test_regression_oleh_receivable_visible_in_repayment_in_flow(self) -> None:
        conn = DebtTextConn()
        self._populate_debt(
            conn,
            debt_id=11,
            counterparty_name="Олег",
            direction="receivable",
            initial_amount="1000.00",
            paid_amount="0",
            remaining_amount="1000.00",
            status="active",
        )
        message = DummyMessage()
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        with patch.object(bot_main, "_pool", return_value=_Pool(conn)), patch.object(
            bot_main, "_user_ready", AsyncMock(return_value=True)
        ), patch.object(bot_main, "_get_user", AsyncMock(return_value=conn.user)):
            summary = await bot_main._debts_report_text(conn, 77)
            await bot_main.debt_callback(_make_callback_update(message, "debt:repay"), context)
            await bot_main.debt_callback(_make_callback_update(message, "debt:repay:in"), context)

        self.assertIn("Мені ще винні", summary)
        self.assertNotIn("-500", summary)
        self.assertNotIn("Активних боргів для цього типу немає", message.replies[-1]["text"])
        labels = _keyboard_labels(message.replies[-1]["reply_markup"])
        self.assertTrue(any("Олег" in label for label in labels))
        self.assertTrue(any("1 000" in label for label in labels))

    async def test_voice_message_shows_debt_fallback(self) -> None:
        conn = DebtTextConn()
        message = DummyMessage(voice_duration=4)
        update = DummyUpdate(message)
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={"debt_flow": {"mode": "create"}})

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        with patch.object(bot_main, "_pool", return_value=_Pool(conn)), patch.object(
            bot_main, "is_user_banned", AsyncMock(return_value=False)
        ), patch.object(bot_main, "should_block_for_maintenance", AsyncMock(return_value=None)), patch.object(
            bot_main, "log_bot_event", AsyncMock()
        ), patch.object(
            bot_main, "transcribe_ogg_bytes", AsyncMock()
        ) as transcribe_mock:
            await bot_main.voice_message(update, context)

        self.assertFalse(transcribe_mock.called)
        self.assertTrue(message.replies)
        self.assertIn("Голосом борги поки не додаються", message.replies[0]["text"])


    async def test_start_entry_with_debt_payload_routes_to_invite_handler(self) -> None:
        conn = DebtTextConn()
        message = DummyMessage(text="/start debt_token123")
        update = DummyUpdate(message)
        update.effective_user.language_code = "uk"
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={}, args=["debt_token123"])
        conversation_end = getattr(bot_main.ConversationHandler, "END", -1)

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        with patch.object(bot_main, "_pool", return_value=_Pool(conn)), patch.object(
            bot_main, "is_user_banned", AsyncMock(return_value=False)
        ), patch.object(
            bot_main, "should_block_for_maintenance", AsyncMock(return_value=None)
        ), patch.object(
            bot_main, "is_registration_open", AsyncMock(return_value=True)
        ), patch.object(
            bot_main, "log_bot_event", AsyncMock()
        ), patch.object(
            bot_main, "_consume_pending_admin_reset_if_needed", AsyncMock()
        ), patch.object(
            bot_main, "_handle_debt_invite_start", AsyncMock(return_value=conversation_end)
        ) as invite_handler:
            result = await bot_main.start_entry(update, context)

        invite_handler.assert_awaited_once()
        self.assertEqual(invite_handler.await_args.args[2], "token123")
        self.assertEqual(result, conversation_end)

    async def test_handle_debt_invite_start_blocks_owner(self) -> None:
        message = DummyMessage()
        update = DummyUpdate(message)
        update.effective_user.id = 77
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        invite = {
            "lender_user_id": 77,
            "invite_status": "pending",
            "debt_status": "active",
            "remaining_amount": Decimal("100.00"),
            "currency": "UAH",
            "initial_amount": Decimal("100.00"),
            "counterparty_name": "Олег",
            "comment": "",
            "lender_first_name": "Owner",
            "lender_username": "owner",
        }
        with patch.object(bot_main, "ConversationHandler", SimpleNamespace(END=-1)), patch.object(
            bot_main, "_pool", return_value=_Pool(object())
        ), patch.object(
            bot_main.DebtService, "get_debt_invite", AsyncMock(return_value=invite)
        ):
            result = await bot_main._handle_debt_invite_start(update, context, "token")

        self.assertTrue(message.replies)
        self.assertIn("Це посилання для людини", message.replies[0]["text"])
        self.assertEqual(result, getattr(bot_main.ConversationHandler, "END", -1))

    async def test_lent_debt_confirm_shows_debt_invite_prompt(self) -> None:
        message = DummyMessage()
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={}),
            user_data={
                "debt_flow": {
                    "mode": "create",
                    "direction": "receivable",
                    "counterparty_name": "Микола",
                    "debt_amount": Decimal("3.00"),
                    "debt_currency": "UAH",
                    "account_id": 1,
                    "comment": "Тестовий борг",
                }
            },
        )

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        result = SimpleNamespace(
            status="completed",
            debt={
                "id": 5,
                "direction": "receivable",
                "counterparty_name": "Микола",
                "currency": "UAH",
                "initial_amount": Decimal("3.00"),
                "paid_amount": Decimal("0.00"),
                "remaining_amount": Decimal("3.00"),
                "comment": "Тестовий борг",
            },
        )
        with patch.object(bot_main, "_pool", return_value=_Pool(object())), patch.object(
            bot_main, "_consume_pending_admin_reset_if_needed", AsyncMock()
        ), patch.object(
            bot_main, "_get_resolved_access_state", AsyncMock(return_value={"access_scope": "personal_full", "access_source": "billing"})
        ), patch.object(
            bot_main, "_user_ready", AsyncMock(return_value=True)
        ), patch.object(
            bot_main, "_get_user", AsyncMock(return_value={"first_name": "Owner", "username": "owner", "base_currency": "UAH"})
        ), patch.object(
            bot_main.DebtService, "create_debt", AsyncMock(return_value=result)
        ):
            await bot_main.debt_callback(_make_callback_update(message, prepare_confirmation(context.user_data["debt_flow"], "debt:confirm")), context)

        reply_text = message.replies[-1]["text"]
        self.assertIn("✅ Борг збережено.", reply_text)
        self.assertIn("Хто дав: ви", reply_text)
        self.assertIn("Хто отримав: Микола", reply_text)
        self.assertIn("Сума боргу: 3 UAH", reply_text)
        self.assertIn("Повернуто: 0 UAH", reply_text)
        self.assertIn("Залишок: 3 UAH", reply_text)
        self.assertIn("Коментар: Тестовий борг", reply_text)
        self.assertIn("Хочете створити посилання для Микола", reply_text)
        labels = _keyboard_labels(message.replies[-1]["reply_markup"])
        self.assertIn("Створити посилання для Микола", labels)
        self.assertIn("Без нагадувань", labels)
        self.assertEqual(context.user_data["debt_flow"]["mode"], "invite_prompt")

    async def test_borrowed_debt_confirm_does_not_show_invite_prompt(self) -> None:
        message = DummyMessage()
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={}),
            user_data={
                "debt_flow": {
                    "mode": "create",
                    "direction": "payable",
                    "counterparty_name": "Ірина",
                    "debt_amount": Decimal("3.00"),
                    "debt_currency": "UAH",
                    "account_id": 1,
                    "comment": "Тестовий борг",
                }
            },
        )

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        result = SimpleNamespace(
            status="completed",
            debt={
                "id": 7,
                "direction": "payable",
                "counterparty_name": "Ірина",
                "currency": "UAH",
                "initial_amount": Decimal("3.00"),
                "paid_amount": Decimal("0.00"),
                "remaining_amount": Decimal("3.00"),
                "comment": "Тестовий борг",
            },
        )
        with patch.object(bot_main, "_pool", return_value=_Pool(object())), patch.object(
            bot_main, "_consume_pending_admin_reset_if_needed", AsyncMock()
        ), patch.object(
            bot_main, "_get_resolved_access_state", AsyncMock(return_value={"access_scope": "personal_full", "access_source": "billing"})
        ), patch.object(
            bot_main, "_user_ready", AsyncMock(return_value=True)
        ), patch.object(
            bot_main, "_get_user", AsyncMock(return_value={"first_name": "Owner", "username": "owner", "base_currency": "UAH"})
        ), patch.object(
            bot_main.DebtService, "create_debt", AsyncMock(return_value=result)
        ), patch.object(
            bot_main.DebtService, "build_debt_detail_text", AsyncMock(return_value="DETAIL")
        ):
            await bot_main.debt_callback(_make_callback_update(message, prepare_confirmation(context.user_data["debt_flow"], "debt:confirm")), context)

        self.assertEqual(message.replies[-1]["text"], "DETAIL")
        self.assertNotIn("Хочеш створити лінк для", message.replies[-1]["text"])
        self.assertNotIn("debt_flow", context.user_data)

    async def test_create_invite_button_generates_deep_link_after_real_confirm_flow(self) -> None:
        message = DummyMessage()
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={}, bot=SimpleNamespace(username="cashflowbot")),
            user_data={
                "debt_flow": {
                    "mode": "create",
                    "direction": "receivable",
                    "counterparty_name": "Микола",
                    "debt_amount": Decimal("3.00"),
                    "debt_currency": "UAH",
                    "account_id": 1,
                    "comment": "Тестовий борг",
                }
            },
        )

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        created_debt = {
            "id": 9,
            "tg_user_id": 77,
            "direction": "receivable",
            "counterparty_name": "Микола",
            "currency": "UAH",
            "initial_amount": Decimal("3.00"),
            "paid_amount": Decimal("0.00"),
            "remaining_amount": Decimal("3.00"),
            "comment": "Тестовий борг",
        }
        create_debt_invite = AsyncMock(
            return_value=SimpleNamespace(status="completed", invite={"token": "tok123"})
        )
        with patch.object(bot_main, "_pool", return_value=_Pool(object())), patch.object(
            bot_main, "_consume_pending_admin_reset_if_needed", AsyncMock()
        ), patch.object(
            bot_main, "_get_resolved_access_state", AsyncMock(return_value={"access_scope": "personal_full", "access_source": "billing"})
        ), patch.object(
            bot_main, "_user_ready", AsyncMock(return_value=True)
        ), patch.object(
            bot_main, "_get_user", AsyncMock(return_value={"first_name": "Owner", "username": "owner", "base_currency": "UAH"})
        ), patch.object(
            bot_main.DebtService, "create_debt", AsyncMock(return_value=SimpleNamespace(status="completed", debt=created_debt))
        ), patch.object(
            bot_main.DebtService, "get_debt", AsyncMock(return_value=created_debt)
        ), patch.object(
            bot_main.DebtService, "create_debt_invite", create_debt_invite
        ), patch.object(
            bot_main, "_resolve_bot_username", AsyncMock(return_value="cashflowbot")
        ):
            await bot_main.debt_callback(_make_callback_update(message, prepare_confirmation(context.user_data["debt_flow"], "debt:confirm")), context)
            await bot_main.debt_callback(_make_callback_update(message, "debt:invite:create"), context)

        create_debt_invite.assert_awaited_once()
        ready_text = message.replies[-1]["text"]
        self.assertIn("Посилання для Микола готове.", ready_text)
        self.assertIn("https://t.me/cashflowbot?start=debt_tok123", ready_text)
        self.assertIn("?start=debt_", ready_text)
        match = re.search(r"https://t\.me/[^\s]+", ready_text)
        self.assertIsNotNone(match)
        deep_link = match.group(0)
        self.assertEqual(deep_link, "https://t.me/cashflowbot?start=debt_tok123")
        self.assertNotIn("Микола", deep_link)
        self.assertNotIn("Тестовий", deep_link)
        self.assertNotIn("77", deep_link)
        labels = _keyboard_labels(message.replies[-1]["reply_markup"])
        self.assertIn("📤 Поділитися повідомленням", labels)
        self.assertIn("До боргу", labels)
        self.assertIn("До меню", labels)
        share_urls = _keyboard_urls(message.replies[-1]["reply_markup"])
        self.assertTrue(share_urls)
        self.assertIn("https://t.me/share/url?text=", share_urls[0])
        self.assertNotIn("?url=", share_urls[0])
        decoded_share_url = unquote(share_urls[0])
        self.assertIn("https://t.me/cashflowbot?start=debt_tok123", decoded_share_url)
        self.assertIn("Привіт! Я зафіксував(ла) у боті борг.", decoded_share_url)
        self.assertNotIn("Лінк для Микола готовий.", decoded_share_url)
        self.assertNotIn("Надішли боржнику повідомлення нижче.", decoded_share_url)
        self.assertNotIn("------", decoded_share_url)

    async def test_invite_confirm_callback_notifies_owner_without_onboarding(self) -> None:
        message = DummyMessage()
        update = _make_callback_update(message, "debt:invite:confirm:tok")
        update.effective_user.id = 99
        update.effective_user.first_name = "Borrower"
        update.effective_user.username = "borrower"
        bot = SimpleNamespace(send_message=AsyncMock())
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}, bot=bot), user_data={})

        class _Acquire:
            def __init__(self, inner):
                self.inner = inner

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, inner):
                self.inner = inner

            def acquire(self):
                return _Acquire(self.inner)

        confirm_result = SimpleNamespace(status="completed", debt={"tg_user_id": 77})
        with patch.object(bot_main, "_pool", return_value=_Pool(object())), patch.object(
            bot_main, "_consume_pending_admin_reset_if_needed", AsyncMock()
        ), patch.object(
            bot_main, "_get_resolved_access_state", AsyncMock(return_value={"access_scope": "personal_full", "access_source": "billing"})
        ), patch.object(
            bot_main, "_clear_pending_start_payload_for_state", AsyncMock()
        ), patch.object(
            bot_main.DebtService, "confirm_debt_invite", AsyncMock(return_value=confirm_result)
        ):
            await bot_main.debt_callback(update, context)

        self.assertIn("Підтвердження збережено", message.replies[-1]["text"])
        bot.send_message.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
