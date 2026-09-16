from __future__ import annotations

import asyncio
import os
import sys
import types
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
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


class DummyMessage:
    def __init__(self, text: str = "") -> None:
        self.text = text
        self.replies: list[dict] = []
        self.chat = SimpleNamespace(send_action=AsyncMock())

    async def reply_text(self, text: str, reply_markup=None, **kwargs) -> None:
        self.replies.append({"text": text, "reply_markup": reply_markup, "kwargs": kwargs})


class DummyQuery:
    def __init__(self, data: str, message: DummyMessage) -> None:
        self.data = data
        self.message = message
        self.answered = False

    async def answer(self) -> None:
        self.answered = True


class DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def __init__(self, *, accounts_by_id: dict[int, dict] | None = None) -> None:
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self.accounts_by_id: dict[int, dict] = {}
        for account_id, raw_account in (accounts_by_id or {}).items():
            account = dict(raw_account)
            account.setdefault("id", int(account_id))
            account.setdefault("tg_user_id", 123)
            account.setdefault("label", f"Account {account_id}")
            account.setdefault("currency", "UAH")
            account.setdefault("balance", Decimal("0"))
            account.setdefault("account_type", "main")
            account.setdefault("credit_limit", None)
            account.setdefault("monthly_interest_rate", None)
            account.setdefault("non_negative_account_type", "main")
            self.accounts_by_id[int(account_id)] = account
        self.daily_expense_day_statuses: dict[tuple[int, object], dict] = {}
        default_expiry = datetime(2030, 1, 1, tzinfo=UTC)
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
            "expires_at": default_expiry,
            "next_charge_at": default_expiry,
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

    async def execute(self, query: str, *args) -> None:
        normalized = " ".join(query.split())
        if "INSERT INTO user_admin_states" in normalized and "access_scope" in normalized:
            self.access_state["access_scope"] = str(args[1])
            self.access_state["access_source"] = str(args[2])
            if len(args) > 3:
                self.access_state["pending_start_payload"] = str(args[3])
            return
        if "INSERT INTO daily_expense_day_statuses" in normalized and "expense_recorded_at" in normalized:
            key = (int(args[0]), args[1])
            existing = self.daily_expense_day_statuses.get(key, {})
            self.daily_expense_day_statuses[key] = {
                **existing,
                "tg_user_id": int(args[0]),
                "day": args[1],
                "expense_recorded_at": True,
                "no_expenses_confirmed_at": None,
            }
            return
        self.execute_calls.append((query, args))
        if normalized.startswith("UPDATE accounts SET balance=$3"):
            account_id = int(args[1])
            balance = Decimal(str(args[2]))
            account = self.accounts_by_id.get(account_id)
            if account is not None:
                account["balance"] = balance
                if len(args) > 3:
                    account["account_type"] = args[3]
                if len(args) > 4:
                    account["credit_limit"] = args[4]
                if len(args) > 5:
                    account["non_negative_account_type"] = args[5]

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "INSERT INTO transactions" in normalized:
            self.execute_calls.append((query, args))
            return sum(1 for saved_query, _ in self.execute_calls if "INSERT INTO transactions" in " ".join(saved_query.split()))
        if "SELECT to_regclass($1) IS NOT NULL" in normalized:
            return str(args[0]) in {"billing_profiles", "subscriptions", "user_admin_states", "promo_offers"}
        if "SELECT 1 FROM users WHERE tg_user_id=$1" in normalized:
            return 1
        if "SELECT count(1) FROM accounts WHERE tg_user_id=$1 AND family_id IS NULL AND is_active=true" in normalized:
            return max(1, len(self.accounts_by_id))
        if "SELECT count(1) FROM accounts WHERE family_id=$1 AND is_active=true" in normalized:
            return 0
        return None

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM family_members" in normalized and "JOIN families" in normalized:
            return None
        if "FROM users" in normalized and "WHERE tg_user_id=$1" in normalized:
            return {
                "tg_user_id": int(args[0]),
                "start_date": datetime.now().date(),
                "base_currency": "UAH",
                "onboarding_completed": True,
                "onboarding_version": bot_main.CURRENT_ONBOARDING_VERSION,
            }
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
        account_id = int(args[1]) if len(args) > 1 else None
        if account_id is None:
            return None
        account = self.accounts_by_id.get(account_id)
        return dict(account) if account is not None else None


class DummyAcquire:
    def __init__(self, conn: DummyConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> DummyConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyPool:
    def __init__(self, conn: DummyConn) -> None:
        self._conn = conn

    def acquire(self) -> DummyAcquire:
        return DummyAcquire(self._conn)


class IncomeFlowTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _button_labels(reply_markup) -> list[str]:
        labels: list[str] = []
        rows = reply_markup.args[0] if reply_markup and getattr(reply_markup, "args", None) else []
        for row in rows:
            for button in row:
                labels.append(button.args[0] if getattr(button, "args", None) else "")
        return labels

    @staticmethod
    def _button_callbacks(reply_markup) -> list[str]:
        callbacks: list[str] = []
        rows = reply_markup.args[0] if reply_markup and getattr(reply_markup, "args", None) else []
        for row in rows:
            for button in row:
                callbacks.append(str(getattr(button, "kwargs", {}).get("callback_data") or ""))
        return callbacks

    def _make_context(self, tx_flow: dict | None = None):
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={}),
            user_data={},
        )
        if tx_flow is not None:
            context.user_data["tx_flow"] = tx_flow
        return context

    def _make_update(self, text: str, callback_data: str | None = None):
        message = DummyMessage(text=text)
        user = SimpleNamespace(id=123, username="tester")
        if callback_data is None:
            return SimpleNamespace(effective_user=user, message=message), message
        query = DummyQuery(callback_data, message)
        return SimpleNamespace(effective_user=user, message=message, callback_query=query), message, query

    async def test_income_amount_prompt_uses_income_examples(self) -> None:
        prompt = bot_main._tx_amount_prompt_text("income", "Main", "Зарплата")
        self.assertIn("500 зарплата", prompt)
        self.assertIn("500 фріланс", prompt)
        self.assertIn("500 повернення боргу", prompt)
        self.assertNotIn("аптека", prompt)

    async def test_income_amount_input_keeps_draft_until_confirmation(self) -> None:
        context = self._make_context(
            {
                "kind": "income",
                "step": "await_amount",
                "account_id": 11,
                "account_label": "Main",
                "category_id": 22,
                "category_label": "Зарплата",
                "currency": "UAH",
                "await_amount": True,
            }
        )
        update, message = self._make_update("500 зарплата")

        await bot_main._save_tx_amount(update, context, "500 зарплата")

        flow = context.user_data["tx_flow"]
        self.assertEqual(flow["account_id"], 11)
        self.assertEqual(flow["category_id"], 22)
        self.assertEqual(flow["step"], bot_main.TX_FLOW_STEP_CONFIRMATION)
        self.assertEqual(flow["amount"], Decimal("500"))
        self.assertEqual(len(message.replies), 1)
        self.assertIn("Підтвердіть дохід", message.replies[0]["text"])

    async def test_income_amount_input_blocks_explicit_currency_mismatch(self) -> None:
        context = self._make_context(
            {
                "kind": "income",
                "step": "await_amount",
                "account_id": 11,
                "account_label": "Main",
                "category_id": 22,
                "category_label": "Зарплата",
                "currency": "UAH",
                "await_amount": True,
            }
        )
        update, message = self._make_update("500 USDT зарплата")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(11, "Main"), (22, "USDT Wallet")])),
        ):
            await bot_main._save_tx_amount(update, context, "500 USDT зарплата")

        flow = context.user_data["tx_flow"]
        resolution_flow = context.user_data["currency_resolution_flow"]
        self.assertEqual(flow["step"], "currency_resolution")
        self.assertEqual(resolution_flow["mode"], "tx")
        self.assertEqual(resolution_flow["source_currency"], "USDT")
        self.assertEqual(resolution_flow["original_amount"], Decimal("500"))
        self.assertEqual(resolution_flow["target_account_id"], 11)
        self.assertNotIn("amount", flow)
        self.assertIn("USDT", message.replies[-1]["text"])
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        callbacks = self._button_callbacks(message.replies[-1]["reply_markup"])
        self.assertTrue(any("Створити рахунок USDT" in label for label in labels))
        self.assertTrue(any("Автоматична конвертація" in label for label in labels))
        self.assertTrue(any("Конвертувати вручну" in label for label in labels))
        self.assertIn("menu:main", callbacks)

    async def test_income_currency_resolution_auto_conversion_prepares_confirmation(self) -> None:
        conn = DummyConn(accounts_by_id={11: {"id": 11, "label": "Main", "currency": "UAH", "balance": Decimal("100")}})
        context = self._make_context(
            {
                "kind": "income",
                "step": "currency_resolution",
                "category_id": 22,
                "category_label": "Зарплата",
            }
        )
        context.user_data["currency_resolution_flow"] = {
            "mode": "tx",
            "step": "menu",
            "callback_prefix": "income:currency",
            "source_currency": "USDT",
            "target_account_id": 11,
            "target_account_label": "Main",
            "target_currency": "UAH",
            "original_amount": Decimal("100"),
            "original_comment": "зарплата",
        }
        update, message, query = self._make_update("", "income:currency:auto")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_resolve_nbu_rate", new=AsyncMock(return_value=Decimal("40"))),
        ):
            await bot_main.pick_callback(update, context)

        self.assertTrue(query.answered)
        flow = context.user_data["tx_flow"]
        self.assertEqual(flow["step"], bot_main.TX_FLOW_STEP_CONFIRMATION)
        self.assertEqual(flow["account_id"], 11)
        self.assertEqual(flow["currency"], "UAH")
        self.assertEqual(flow["amount"], Decimal("4000.00"))
        self.assertNotIn("currency_resolution_flow", context.user_data)
        self.assertIn("Курс", message.replies[-1]["text"])
        self.assertIn("Підтвердіть дохід", message.replies[-1]["text"])

    async def test_income_cancel_clears_state_without_writes(self) -> None:
        conn = DummyConn()
        context = self._make_context(
            {
                "kind": "income",
                "step": bot_main.TX_FLOW_STEP_CONFIRMATION,
                "account_id": 11,
                "account_label": "Main",
                "category_id": 22,
                "category_label": "Зарплата",
                "amount": Decimal("500"),
                "currency": "UAH",
            }
        )
        update, message, query = self._make_update("", "income:cancel")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_reply_home", new=AsyncMock()),
        ):
            await bot_main.pick_callback(update, context)

        self.assertTrue(query.answered)
        self.assertNotIn("tx_flow", context.user_data)
        self.assertEqual(conn.execute_calls, [])

    async def test_income_confirm_writes_once_and_updates_balance(self) -> None:
        account = {"id": 11, "label": "Main", "currency": "UAH", "balance": Decimal("100")}
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context(
            {
                "kind": "income",
                "step": bot_main.TX_FLOW_STEP_CONFIRMATION,
                "account_id": 11,
                "account_label": "Main",
                "category_id": 22,
                "category_label": "Зарплата",
                "amount": Decimal("25"),
                "currency": "UAH",
                "comment": "зарплата",
            }
        )
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["tx_flow"], "income:confirm"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "TransactionService", wraps=bot_main.TransactionService) as service_cls,
        ):
            await bot_main.pick_callback(update, context)

        self.assertTrue(query.answered)
        self.assertNotIn("tx_flow", context.user_data)
        service_cls.assert_called_once()
        self.assertEqual(len(conn.execute_calls), 2)
        self.assertIn("INSERT INTO transactions", conn.execute_calls[0][0])
        self.assertIn("UPDATE accounts", conn.execute_calls[1][0])
        self.assertEqual(conn.execute_calls[0][1][-1], "Зарплата")
        self.assertEqual(conn.execute_calls[1][1][2], Decimal("125.00"))
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("125.00"))
        self.assertEqual(conn.daily_expense_day_statuses, {})
        self.assertIn("Дохід збережено", message.replies[-1]["text"])
        callbacks = [button.kwargs.get("callback_data") for row in message.replies[-1]["reply_markup"].args[0] for button in row]
        self.assertIn("txvoid:pick:1", callbacks)

    async def test_expense_confirm_marks_daily_expense_status(self) -> None:
        account = {"id": 11, "label": "Main", "currency": "UAH", "balance": Decimal("500")}
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context(
            {
                "kind": "expense",
                "step": bot_main.TX_FLOW_STEP_CONFIRMATION,
                "account_id": 11,
                "account_label": "Main",
                "category_id": 21,
                "category_label": "Taxi",
                "amount": Decimal("25"),
                "currency": "UAH",
                "comment": "taxi",
            }
        )
        update, _message, query = self._make_update("", prepare_confirmation(context.user_data["tx_flow"], "expense:confirm"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
        ):
            await bot_main.pick_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(len(conn.daily_expense_day_statuses), 1)
        [(status_key, status_value)] = list(conn.daily_expense_day_statuses.items())
        self.assertEqual(status_key[0], 123)
        self.assertTrue(status_value["expense_recorded_at"])

    async def test_ai_confirm_writes_once_and_uses_transaction_service(self) -> None:
        account = {"id": 11, "label": "Main", "currency": "UAH", "balance": Decimal("100")}
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        context.user_data["ai_tx_flow"] = {
            "tx": {
                "date": "2026-01-20",
                "type": "income",
                "amount": Decimal("25"),
                "account_id": 11,
                "category_id": 22,
                "comment": "зарплата",
                "source": "rules",
            },
            "categories": {22: "Зарплата"},
        }
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["ai_tx_flow"], "ai:tx:ok"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_reply_home", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "TransactionService", wraps=bot_main.TransactionService) as service_cls,
        ):
            await bot_main.ai_callback(update, context)

        self.assertTrue(query.answered)
        self.assertNotIn("ai_tx_flow", context.user_data)
        service_cls.assert_called_once()
        self.assertEqual(len(conn.execute_calls), 2)
        callbacks = [button.kwargs.get("callback_data") for row in message.replies[-1]["reply_markup"].args[0] for button in row]
        self.assertIn("txvoid:pick:1", callbacks)
        self.assertIn("INSERT INTO transactions", conn.execute_calls[0][0])
        self.assertIn("UPDATE accounts", conn.execute_calls[1][0])
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("125.00"))
        self.assertIn("Операцію збережено", message.replies[-1]["text"])

        self.assertEqual(conn.daily_expense_day_statuses, {})

    async def test_ai_text_flow_defaults_to_single_account_when_account_not_parsed(self) -> None:
        context = self._make_context()
        update, _message = self._make_update("coffee 100")
        parsed = SimpleNamespace(
            is_candidate_tx=True,
            intent="expense",
            date=None,
            amount=Decimal("100"),
            currency="UAH",
            currency_explicit=False,
            account_id=None,
            category_id="cafes_restaurants_delivery",
            comment="coffee",
            confidence=0.9,
        )
        catalog = {
            "accounts_rows": [(11, "Main")],
            "accounts_full": [{"id": 11, "label": "Main", "currency": "UAH", "account_type": "main"}],
            "expense_full": [(21, "Кафе, ресторани, доставка", ["cafe", "coffee"])],
            "income_full": [(22, "Salary", ["salary"])],
            "expense_rows": [(21, "Кафе, ресторани, доставка")],
            "income_rows": [(22, "Salary")],
            "accounts_map": {11: "Main"},
            "categories_map": {21: "Кафе, ресторани, доставка", 22: "Salary"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=[]),
            patch.object(bot_main, "parse_message", return_value=parsed),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "coffee 100")

        self.assertTrue(handled)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["account_id"], 11)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["currency"], "UAH")

    async def test_ai_text_flow_picks_single_try_account_from_currency_word_form(self) -> None:
        context = self._make_context()
        update, _message = self._make_update("10 лір молоко")
        catalog = {
            "accounts_rows": [(11, "Готівка (TRY)"), (22, "PrivatBank •3882 (UAH)")],
            "accounts_full": [
                {"id": 11, "label": "Готівка (TRY)", "currency": "TRY", "account_type": "cash"},
                {"id": 22, "label": "PrivatBank •3882 (UAH)", "currency": "UAH", "account_type": "main"},
            ],
            "expense_full": [(21, "Продукти", ["groceries"])],
            "income_full": [(31, "Salary", ["salary"])],
            "expense_rows": [(21, "Продукти")],
            "income_rows": [(31, "Salary")],
            "accounts_map": {11: "Готівка (TRY)", 22: "PrivatBank •3882 (UAH)"},
            "categories_map": {21: "Продукти", 31: "Salary"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=[]),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "10 лір молоко")

        self.assertTrue(handled)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["account_id"], 11)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["currency"], "TRY")

    async def test_ai_text_flow_picks_only_uah_account_when_currency_is_explicit(self) -> None:
        context = self._make_context()
        update, _message = self._make_update("купив хліб одна гривня")
        catalog = {
            "accounts_rows": [(11, "PrivatBank •3882 (UAH)"), (22, "Готівка (USD)")],
            "accounts_full": [
                {"id": 11, "label": "PrivatBank •3882 (UAH)", "currency": "UAH", "account_type": "main"},
                {"id": 22, "label": "Готівка (USD)", "currency": "USD", "account_type": "cash"},
            ],
            "expense_full": [(21, "Продукти", ["groceries"])],
            "income_full": [(31, "Salary", ["salary"])],
            "expense_rows": [(21, "Продукти")],
            "income_rows": [(31, "Salary")],
            "accounts_map": {11: "PrivatBank •3882 (UAH)", 22: "Готівка (USD)"},
            "categories_map": {21: "Продукти", 31: "Salary"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "USD"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=[]),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "купив хліб одна гривня")

        self.assertTrue(handled)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["account_id"], 11)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["currency"], "UAH")

    async def test_ai_batch_flow_defaults_to_single_account_when_account_not_parsed(self) -> None:
        context = self._make_context()
        update, _message = self._make_update("coffee 100, bread 50")
        whole_message = SimpleNamespace(account_id=None, currency="UAH")
        batch_candidates = [
            SimpleNamespace(
                intent="expense",
                date=None,
                amount=Decimal("100"),
                currency="UAH",
                currency_explicit=False,
                category_id="cafes_restaurants_delivery",
                comment="coffee",
                raw_text="coffee 100",
            ),
            SimpleNamespace(
                intent="expense",
                date=None,
                amount=Decimal("50"),
                currency="UAH",
                currency_explicit=False,
                category_id="groceries",
                comment="bread",
                raw_text="bread 50",
            ),
        ]
        catalog = {
            "accounts_rows": [(11, "Main")],
            "accounts_full": [{"id": 11, "label": "Main", "currency": "UAH", "account_type": "main"}],
            "expense_full": [
                (21, "Кафе, ресторани, доставка", ["cafe", "coffee"]),
                (23, "Продукти", ["grocery", "bread"]),
            ],
            "income_full": [(22, "Salary", ["salary"])],
            "expense_rows": [(21, "Кафе, ресторани, доставка"), (23, "Продукти")],
            "income_rows": [(22, "Salary")],
            "accounts_map": {11: "Main"},
            "categories_map": {21: "Кафе, ресторани, доставка", 22: "Salary", 23: "Продукти"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=batch_candidates),
            patch.object(bot_main, "parse_message", return_value=whole_message),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "coffee 100, bread 50")

        self.assertTrue(handled)
        self.assertEqual(context.user_data["ai_batch_tx_flow"]["account_id"], 11)
        self.assertEqual(
            [item["currency"] for item in context.user_data["ai_batch_tx_flow"]["items"]],
            ["UAH", "UAH"],
        )

    async def test_ai_batch_flow_does_not_default_to_single_mismatched_account_when_currency_explicit(self) -> None:
        context = self._make_context()
        update, _message = self._make_update("doctor 2000 hryvnia, pharmacy 500 hryvnia")
        whole_message = SimpleNamespace(account_id=None, currency="UAH", currency_explicit=True)
        batch_candidates = [
            SimpleNamespace(
                intent="expense",
                date=None,
                amount=Decimal("2000"),
                currency="UAH",
                currency_explicit=True,
                category_id="health",
                comment="doctor",
                raw_text="doctor 2000 hryvnia",
            ),
            SimpleNamespace(
                intent="expense",
                date=None,
                amount=Decimal("500"),
                currency="UAH",
                currency_explicit=True,
                category_id="health",
                comment="pharmacy",
                raw_text="pharmacy 500 hryvnia",
            ),
        ]
        catalog = {
            "accounts_rows": [(11, "USDT Wallet")],
            "accounts_full": [{"id": 11, "label": "USDT Wallet", "currency": "USDT", "account_type": "main"}],
            "expense_full": [(31, "Health", ["doctor", "pharmacy"])],
            "income_full": [(22, "Salary", ["salary"])],
            "expense_rows": [(31, "Health")],
            "income_rows": [(22, "Salary")],
            "accounts_map": {11: "USDT Wallet"},
            "categories_map": {31: "Health", 22: "Salary"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "USD"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=batch_candidates),
            patch.object(bot_main, "parse_message", return_value=whole_message),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "doctor 2000 hryvnia, pharmacy 500 hryvnia")

        self.assertTrue(handled)
        self.assertIsNone(context.user_data["ai_batch_tx_flow"]["account_id"])
        self.assertEqual(
            [item["currency"] for item in context.user_data["ai_batch_tx_flow"]["items"]],
            ["UAH", "UAH"],
        )

    async def test_ai_text_flow_maps_bread_phrase_to_groceries(self) -> None:
        context = self._make_context()
        update, _message = self._make_update("купив хліб 100 грн")
        catalog = {
            "accounts_rows": [(11, "Main")],
            "accounts_full": [{"id": 11, "label": "Main", "currency": "UAH", "account_type": "main"}],
            "expense_full": [(21, "Продукти", ["grocery", "магазин"])],
            "income_full": [(22, "Salary", ["salary"])],
            "expense_rows": [(21, "Продукти")],
            "income_rows": [(22, "Salary")],
            "accounts_map": {11: "Main"},
            "categories_map": {21: "Продукти", 22: "Salary"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=[]),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "купив хліб 100 грн")

        self.assertTrue(handled)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["account_id"], 11)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["category_id"], 21)

    async def test_ai_text_flow_accepts_signed_expense_shorthand_from_home(self) -> None:
        context = self._make_context()
        update, _message = self._make_update("- 150 \u0433\u0440\u043d - \u043f\u0440\u043e\u0434\u0443\u043a\u0442\u0438")
        catalog = {
            "accounts_rows": [(11, "Main")],
            "accounts_full": [{"id": 11, "label": "Main", "currency": "UAH", "account_type": "main"}],
            "expense_full": [(21, "Groceries", ["groceries"])],
            "income_full": [(22, "Salary", ["salary"])],
            "expense_rows": [(21, "Groceries")],
            "income_rows": [(22, "Salary")],
            "accounts_map": {11: "Main"},
            "categories_map": {21: "Groceries", 22: "Salary"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "USD"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=[]),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "- 150 \u0433\u0440\u043d - \u043f\u0440\u043e\u0434\u0443\u043a\u0442\u0438")

        self.assertTrue(handled)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["account_id"], 11)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["amount"], 150.0)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["currency"], "UAH")
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["category_id"], 21)

    async def test_ai_text_flow_accepts_signed_amount_only_as_expense_candidate(self) -> None:
        context = self._make_context()
        update, message = self._make_update("-1677")
        catalog = {
            "accounts_rows": [(11, "Main")],
            "accounts_full": [{"id": 11, "label": "Main", "currency": "UAH", "account_type": "main"}],
            "expense_full": [(21, "Groceries", ["groceries"])],
            "income_full": [(22, "Salary", ["salary"])],
            "expense_rows": [(21, "Groceries")],
            "income_rows": [(22, "Salary")],
            "accounts_map": {11: "Main"},
            "categories_map": {21: "Groceries", 22: "Salary"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=[]),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "-1677")

        self.assertTrue(handled)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["account_id"], 11)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["amount"], 1677.0)
        self.assertIsNone(context.user_data["ai_tx_flow"]["tx"]["category_id"])
        self.assertTrue(message.replies)

    async def test_ai_text_flow_maps_kommunalka_phrase_to_utilities(self) -> None:
        context = self._make_context()
        update, _message = self._make_update("комуналка 100 грн")
        catalog = {
            "accounts_rows": [(11, "Main")],
            "accounts_full": [{"id": 11, "label": "Main", "currency": "UAH", "account_type": "main"}],
            "expense_full": [(31, "Комунальні платежі", ["utilities", "комуналка"])],
            "income_full": [(22, "Salary", ["salary"])],
            "expense_rows": [(31, "Комунальні платежі")],
            "income_rows": [(22, "Salary")],
            "accounts_map": {11: "Main"},
            "categories_map": {31: "Комунальні платежі", 22: "Salary"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "parse_message_batch", return_value=[]),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "комуналка 100 грн")

        self.assertTrue(handled)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["account_id"], 11)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["category_id"], 31)

    async def test_ai_pick_account_applies_account_currency_when_currency_implicit(self) -> None:
        account = {"id": 22, "label": "USDT Wallet", "currency": "USDT", "balance": Decimal("0")}
        conn = DummyConn(accounts_by_id={22: account})
        context = self._make_context()
        context.user_data["ai_tx_flow"] = {
            "tx": {
                "date": "2026-01-20",
                "type": "income",
                "amount": Decimal("25"),
                "currency": "UAH",
                "currency_explicit": False,
                "category_id": 22,
                "comment": "зарплата",
                "source": "rules",
            },
            "categories": {22: "Зарплата"},
        }
        update, _message, query = self._make_update("", "ai:pick:acct:22")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(22, "USDT Wallet")])),
        ):
            await bot_main.ai_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["account_id"], 22)
        self.assertEqual(context.user_data["ai_tx_flow"]["tx"]["currency"], "USDT")

    async def test_ai_confirm_blocks_explicit_currency_mismatch(self) -> None:
        account = {"id": 11, "label": "Main", "currency": "UAH", "balance": Decimal("100")}
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        context.user_data["ai_tx_flow"] = {
            "tx": {
                "date": "2026-01-20",
                "type": "income",
                "amount": Decimal("25"),
                "currency": "USDT",
                "currency_explicit": True,
                "account_id": 11,
                "category_id": 22,
                "comment": "зарплата",
                "source": "rules",
            },
            "categories": {22: "Зарплата"},
        }
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["ai_tx_flow"], "ai:tx:ok"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(11, "Main"), (22, "USDT Wallet")])),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
        ):
            await bot_main.ai_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("ai_tx_flow", context.user_data)
        resolution_flow = context.user_data["currency_resolution_flow"]
        self.assertEqual(conn.execute_calls, [])
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("100"))
        self.assertEqual(resolution_flow["mode"], "ai_tx")
        self.assertEqual(resolution_flow["source_currency"], "USDT")
        self.assertEqual(resolution_flow["target_account_id"], 11)
        self.assertIn("USDT", message.replies[-1]["text"])
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertTrue(any("Створити рахунок USDT" in label for label in labels))
        self.assertTrue(any("Автоматична конвертація" in label for label in labels))

    async def test_ai_confirm_without_account_opens_currency_resolution_menu_for_explicit_currency(self) -> None:
        context = self._make_context()
        context.user_data["ai_tx_flow"] = {
            "tx": {
                "date": "2026-01-20",
                "type": "income",
                "amount": Decimal("25"),
                "currency": "USDT",
                "currency_explicit": True,
                "account_id": None,
                "category_id": 22,
                "comment": "зарплата",
                "source": "rules",
            },
            "accounts": {11: "Main"},
            "categories": {22: "Зарплата"},
        }
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["ai_tx_flow"], "ai:tx:ok"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(
                bot_main,
                "_get_accounts_full",
                new=AsyncMock(return_value=[{"id": 11, "label": "Main", "currency": "UAH", "account_type": "main"}]),
            ),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(11, "Main")])),
            patch.object(bot_main, "_sync_ai_draft_flow", new=AsyncMock()),
        ):
            await bot_main.ai_callback(update, context)

        self.assertTrue(query.answered)
        resolution_flow = context.user_data["currency_resolution_flow"]
        self.assertEqual(resolution_flow["mode"], "ai_tx")
        self.assertEqual(resolution_flow["source_currency"], "USDT")
        self.assertEqual(resolution_flow["target_account_id"], 11)
        self.assertEqual(resolution_flow["target_currency"], "UAH")
        self.assertIn("USDT", message.replies[-1]["text"])

    async def test_ai_batch_confirm_writes_multiple_transactions(self) -> None:
        account = {"id": 11, "label": "Main", "currency": "UAH", "balance": Decimal("1000")}
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        context.user_data["ai_batch_tx_flow"] = {
            "account_id": 11,
            "origin": "voice",
            "raw_text": "batch",
            "categories": {21: "Transport", 22: "Cafe", 23: "Products"},
            "accounts": {11: "Main"},
            "items": [
                {"date": "2026-01-20", "type": "expense", "amount": Decimal("200"), "currency": "UAH", "category_id": 21, "comment": "taxi", "source": "rules"},
                {"date": "2026-01-20", "type": "expense", "amount": Decimal("100"), "currency": "UAH", "category_id": 22, "comment": "coffee", "source": "rules"},
                {"date": "2026-01-20", "type": "expense", "amount": Decimal("500"), "currency": "UAH", "category_id": 23, "comment": "groceries", "source": "rules"},
            ],
        }
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["ai_batch_tx_flow"], "ai:batch:ok"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "TransactionService", wraps=bot_main.TransactionService) as service_cls,
        ):
            await bot_main.ai_callback(update, context)

        self.assertTrue(query.answered)
        self.assertNotIn("ai_batch_tx_flow", context.user_data)
        service_cls.assert_called_once()
        self.assertEqual(len(conn.execute_calls), 6)
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("200.00"))
        self.assertIn("Операцій збережено: 3", message.replies[-1]["text"])

        self.assertEqual(len(conn.daily_expense_day_statuses), 1)
        [(status_key, status_value)] = list(conn.daily_expense_day_statuses.items())
        self.assertEqual(status_key, (123, datetime(2026, 1, 20).date()))
        self.assertTrue(status_value["expense_recorded_at"])

    async def test_ai_batch_pick_account_applies_account_currency_to_implicit_items(self) -> None:
        account = {"id": 22, "label": "USDT Wallet", "currency": "USDT", "balance": Decimal("0")}
        conn = DummyConn(accounts_by_id={22: account})
        context = self._make_context()
        context.user_data["ai_batch_tx_flow"] = {
            "categories": {21: "Transport"},
            "items": [
                {
                    "date": "2026-01-20",
                    "type": "expense",
                    "amount": Decimal("200"),
                    "currency": "UAH",
                    "currency_explicit": False,
                    "category_id": 21,
                    "comment": "taxi",
                    "source": "rules",
                }
            ],
        }
        update, _message, query = self._make_update("", "ai:batch:pick:acct:22")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(22, "USDT Wallet")])),
        ):
            await bot_main.ai_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(context.user_data["ai_batch_tx_flow"]["account_id"], 22)
        self.assertEqual(context.user_data["ai_batch_tx_flow"]["items"][0]["currency"], "USDT")

    async def test_ai_batch_confirm_blocks_explicit_currency_mismatch(self) -> None:
        account = {"id": 11, "label": "Main", "currency": "UAH", "balance": Decimal("1000")}
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        context.user_data["ai_batch_tx_flow"] = {
            "account_id": 11,
            "origin": "voice",
            "raw_text": "batch",
            "categories": {21: "Transport"},
            "accounts": {11: "Main"},
            "items": [
                {
                    "date": "2026-01-20",
                    "type": "expense",
                    "amount": Decimal("200"),
                    "currency": "USDT",
                    "currency_explicit": True,
                    "category_id": 21,
                    "comment": "taxi",
                    "source": "rules",
                }
            ],
        }
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["ai_batch_tx_flow"], "ai:batch:ok"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(11, "Main"), (22, "USDT Wallet")])),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.ai_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("ai_batch_tx_flow", context.user_data)
        resolution_flow = context.user_data["currency_resolution_flow"]
        self.assertEqual(conn.execute_calls, [])
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("1000"))
        self.assertEqual(resolution_flow["mode"], "ai_batch")
        self.assertEqual(resolution_flow["source_currency"], "USDT")
        self.assertEqual(resolution_flow["target_account_id"], 11)
        self.assertIn("USDT", message.replies[-1]["text"])
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertTrue(any("Створити рахунок USDT" in label for label in labels))
        self.assertTrue(any("Автоматична конвертація" in label for label in labels))

    async def test_text_on_confirmation_screen_only_prompts_button(self) -> None:
        conn = DummyConn()
        context = self._make_context(
            {
                "kind": "income",
                "step": bot_main.TX_FLOW_STEP_CONFIRMATION,
                "account_id": 11,
                "account_label": "Main",
                "category_id": 22,
                "category_label": "Зарплата",
                "amount": Decimal("25"),
                "currency": "UAH",
            }
        )
        update, message = self._make_update("будь-що")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(update, context)

        self.assertIn("натисніть", message.replies[0]["text"])
        self.assertEqual(conn.execute_calls, [])
        self.assertEqual(context.user_data["tx_flow"]["account_id"], 11)

    async def test_double_confirm_does_not_duplicate(self) -> None:
        account = {"id": 11, "label": "Main", "currency": "UAH", "balance": Decimal("100")}
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context(
            {
                "kind": "income",
                "step": bot_main.TX_FLOW_STEP_CONFIRMATION,
                "account_id": 11,
                "account_label": "Main",
                "category_id": 22,
                "category_label": "Зарплата",
                "amount": Decimal("25"),
                "currency": "UAH",
            }
        )
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["tx_flow"], "income:confirm"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "TransactionService", wraps=bot_main.TransactionService) as service_cls,
        ):
            await bot_main.pick_callback(update, context)
            await bot_main.pick_callback(update, context)

        service_cls.assert_called_once()
        self.assertEqual(len(conn.execute_calls), 2)
        self.assertIn("Дохід збережено", message.replies[0]["text"])
        self.assertEqual(message.replies[-1]["text"], "Ця дія вже неактивна.")
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("125.00"))


    async def test_after_income_prompt_appears_when_savings_account_exists(self) -> None:
        context = self._make_context()
        message = DummyMessage()
        transaction_result = SimpleNamespace(
            status="completed",
            kind="income",
            amount=Decimal("1000"),
            currency="UAH",
            account={"id": 11, "label": "Monobank +4444"},
            new_balance=Decimal("1000"),
        )

        class FakeSavingsService:
            def __init__(self, conn) -> None:
                self.conn = conn

            async def get_or_create_settings(self, tg_user_id):
                return {
                    "enabled": True,
                    "ask_after_income": True,
                    "ask_only_for_salary": False,
                    "default_percent": Decimal("15"),
                    "secondary_percent": Decimal("5"),
                    "default_target_account_id": 22,
                    "min_income_amount": None,
                }

            async def get_active_target_accounts(self, tg_user_id):
                return [
                    {
                        "id": 22,
                        "label": "Банка Подушка",
                        "currency": "UAH",
                        "account_type": "savings",
                        "balance": Decimal("0"),
                    }
                ]

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "SavingsService", FakeSavingsService),
        ):
            prompted = await bot_main._maybe_prompt_saving_after_income(
                message,
                context,
                123,
                transaction_result=transaction_result,
                category_id=22,
            )

        self.assertTrue(prompted)
        self.assertIn("150 UAH", message.replies[0]["text"])
        labels = self._button_labels(message.replies[0]["reply_markup"])
        self.assertIn("Відкласти 150 UAH", labels)
        self.assertNotIn("Запланувати 5% — 50 UAH", labels)

    async def test_after_income_setup_prompt_appears_when_no_savings_account_exists(self) -> None:
        context = self._make_context()
        message = DummyMessage()
        transaction_result = SimpleNamespace(
            status="completed",
            kind="income",
            amount=Decimal("1000"),
            currency="UAH",
            account={"id": 11, "label": "Monobank +4444"},
            new_balance=Decimal("1000"),
        )

        class FakeSavingsService:
            def __init__(self, conn) -> None:
                self.conn = conn

            async def get_or_create_settings(self, tg_user_id):
                return {
                    "enabled": True,
                    "ask_after_income": True,
                    "ask_only_for_salary": False,
                    "default_percent": Decimal("15"),
                    "secondary_percent": None,
                    "default_target_account_id": None,
                    "min_income_amount": None,
                }

            async def get_active_target_accounts(self, tg_user_id):
                return []

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "SavingsService", FakeSavingsService),
        ):
            prompted = await bot_main._maybe_prompt_saving_after_income(
                message,
                context,
                123,
                transaction_result=transaction_result,
                category_id=22,
            )

        self.assertTrue(prompted)
        self.assertIn("створіть перше накопичення", message.replies[0]["text"].lower())
        labels = self._button_labels(message.replies[0]["reply_markup"])
        self.assertIn("Створити накопичення", labels)

    async def test_post_income_account_create_text_routes_to_account_create_before_saving_flow(self) -> None:
        context = self._make_context()
        context.user_data["saving_flow"] = {
            "mode": "post_income_prompt",
            "source_account_id": 11,
            "primary_amount": Decimal("150"),
        }
        context.user_data["settings_flow"] = {
            "mode": "account_create",
            "step": "waiting_for_account_name",
            "origin": "saving",
        }
        update, _message = self._make_update("Банка Подушка")
        conn = DummyConn()
        handle_account_create = AsyncMock()
        handle_saving = AsyncMock()

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_handle_account_create_text", new=handle_account_create),
            patch.object(bot_main, "_handle_saving_text", new=handle_saving),
        ):
            await bot_main.text_message(update, context)

        handle_account_create.assert_awaited_once()
        handle_saving.assert_not_called()

    async def test_post_income_confirmed_transfer_uses_savings_transfer_without_pending_task(self) -> None:
        context = self._make_context()
        context.user_data["saving_flow"] = {
            "mode": "post_income_prompt",
            "source_account_id": 11,
            "source_label": "Monobank +4444",
            "source_currency": "UAH",
            "target_account_id": 22,
            "target_label": "На подорож",
            "target_currency": "UAH",
            "target_balance": Decimal("0"),
            "target_goal_amount": Decimal("10000"),
            "amount": Decimal("150"),
        }
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["saving_flow"], "saving:topup:confirm:ok"))
        calls: list[dict] = []

        class FakeSavingsService:
            def __init__(self, conn) -> None:
                self.conn = conn

            async def record_completed_transfer(self, tg_user_id, **kwargs):
                calls.append(kwargs)
                return SimpleNamespace(
                    status="completed",
                    source_balance=Decimal("1000"),
                    target_amount=Decimal("150"),
                )

            async def create_pending_task(self, *args, **kwargs):
                raise AssertionError("post-income flow must not create PendingSavingTask")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "SavingsService", FakeSavingsService),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.saving_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(calls[0]["transfer_subtype"], "savings_transfer")
        self.assertIn("Monobank +4444: 850 UAH", message.replies[0]["text"])
        self.assertIn("На подорож: 150 UAH / 10 000 UAH", message.replies[0]["text"])

    async def test_saving_add_account_create_does_not_resume_zero_income_prompt(self) -> None:
        context = self._make_context()
        context.user_data["saving_flow"] = {"mode": "add_account"}
        context.user_data["settings_flow"] = {
            "mode": "account_create",
            "origin": "saving",
            "account": {
                "name": "Бінанс",
                "currency": "USD",
                "account_type": "investment",
                "initial_balance": Decimal("10"),
                "goal_amount": Decimal("1000"),
            },
        }
        update, _message, query = self._make_update("", "settings:acct:confirm:create")
        overview_mock = AsyncMock()
        saving_prompt_mock = AsyncMock()
        topup_mock = AsyncMock()
        create_plan_mock = AsyncMock()

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_accounts_full", new=AsyncMock(return_value=[])),
            patch.object(bot_main.AccountService, "create_account", new=AsyncMock(return_value=77)),
            patch.object(bot_main, "_show_savings_overview", new=overview_mock),
            patch.object(bot_main, "_show_saving_income_prompt", new=saving_prompt_mock),
            patch.object(bot_main, "_show_topup_source_picker", new=topup_mock),
            patch.object(bot_main, "_create_pending_saving_plan", new=create_plan_mock),
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        overview_mock.assert_awaited_once()
        saving_prompt_mock.assert_not_called()
        topup_mock.assert_not_called()
        create_plan_mock.assert_not_called()
        self.assertIsNone(context.user_data.get("saving_flow"))

    async def test_post_income_account_create_with_zero_amount_falls_back_to_overview(self) -> None:
        context = self._make_context()
        context.user_data["saving_flow"] = {
            "mode": "post_income_prompt",
            "source_account_id": 11,
            "source_currency": "UAH",
            "primary_amount": Decimal("0"),
        }
        context.user_data["settings_flow"] = {
            "mode": "account_create",
            "origin": "saving",
            "account": {
                "name": "Подушка USD",
                "currency": "USD",
                "account_type": "investment",
                "initial_balance": Decimal("10"),
            },
        }
        update, _message, query = self._make_update("", "settings:acct:confirm:create")
        overview_mock = AsyncMock()
        confirm_mock = AsyncMock()
        saving_prompt_mock = AsyncMock()
        created_account = {
            "id": 77,
            "label": "Подушка USD",
            "currency": "USD",
            "account_type": "investment",
            "balance": Decimal("10"),
            "goal_amount": None,
            "goal_date": None,
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_accounts_full", new=AsyncMock(return_value=[])),
            patch.object(bot_main.AccountService, "create_account", new=AsyncMock(return_value=77)),
            patch.object(bot_main, "_get_active_account_by_id", new=AsyncMock(return_value=created_account)),
            patch.object(bot_main, "_show_savings_overview", new=overview_mock),
            patch.object(bot_main, "_show_saving_transfer_confirmation", new=confirm_mock),
            patch.object(bot_main, "_show_saving_income_prompt", new=saving_prompt_mock),
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        overview_mock.assert_awaited_once()
        confirm_mock.assert_not_called()
        saving_prompt_mock.assert_not_called()

    async def test_disable_prompt_after_income_sets_ask_after_income_false(self) -> None:
        context = self._make_context()
        update, message, query = self._make_update("", "saving:disable_prompt")
        calls: list[dict] = []

        class FakeSavingsService:
            def __init__(self, conn) -> None:
                self.conn = conn

            async def update_settings(self, tg_user_id, **fields):
                calls.append(fields)
                return {}

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "SavingsService", FakeSavingsService),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.saving_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(calls[0]["ask_after_income"], False)
        self.assertIn("більше не пропонуватиму", message.replies[0]["text"])

    async def test_saving_overview_callback_opens_overview_not_settings(self) -> None:
        context = self._make_context()
        update, _message, query = self._make_update("", "saving:overview")
        overview_mock = AsyncMock()
        settings_mock = AsyncMock()

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(DummyConn())),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_savings_overview", new=overview_mock),
            patch.object(bot_main, "_show_saving_settings", new=settings_mock),
        ):
            await bot_main.saving_callback(update, context)

        self.assertTrue(query.answered)
        overview_mock.assert_awaited()
        settings_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
