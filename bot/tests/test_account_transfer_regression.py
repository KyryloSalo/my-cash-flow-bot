from __future__ import annotations

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
    def __init__(
        self,
        *,
        fetch_rows: list[dict] | None = None,
        accounts_by_id: dict[int, dict] | None = None,
    ) -> None:
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self.fetch_rows = fetch_rows or []
        self.accounts_by_id = accounts_by_id or {}
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
        self.execute_calls.append((query, args))

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        return list(self.fetch_rows)

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT to_regclass($1) IS NOT NULL" in normalized:
            return str(args[0]) in {"billing_profiles", "subscriptions", "user_admin_states", "promo_offers"}
        if "SELECT 1 FROM users WHERE tg_user_id=$1" in normalized:
            return 1
        if "SELECT count(1) FROM accounts WHERE tg_user_id=$1 AND family_id IS NULL AND is_active=true" in normalized:
            return max(1, len(self.accounts_by_id) or len(self.fetch_rows))
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
        if normalized.startswith("INSERT INTO accounts"):
            return {"id": 1}
        account_id = int(args[1]) if len(args) > 1 else None
        if account_id is None:
            return None
        return self.accounts_by_id.get(account_id)


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


class AccountAndTransferRegressionTests(unittest.IsolatedAsyncioTestCase):
    def _make_context(self, *, settings_flow: dict | None = None, transfer_flow: dict | None = None):
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={}),
            user_data={},
        )
        if settings_flow is not None:
            context.user_data["settings_flow"] = settings_flow
        if transfer_flow is not None:
            context.user_data["transfer_flow"] = transfer_flow
        return context

    def _make_update(self, text: str, callback_data: str | None = None):
        message = DummyMessage(text=text)
        user = SimpleNamespace(id=123, username="tester")
        if callback_data is None:
            return SimpleNamespace(effective_user=user, message=message), message
        query = DummyQuery(callback_data, message)
        return SimpleNamespace(effective_user=user, message=message, callback_query=query), message, query

    async def test_get_accounts_queries_only_active_accounts(self) -> None:
        conn = DummyConn(fetch_rows=[{"id": 1, "label": "Main", "currency": "UAH"}])

        rows = await bot_main._get_accounts(conn, 123)

        self.assertEqual(rows, [(1, "Main (UAH)")])
        self.assertEqual(len(conn.fetch_calls), 1)
        self.assertIn("is_active=true", conn.fetch_calls[0][0])

    async def test_account_create_success_inserts_active_account(self) -> None:
        conn = DummyConn(fetch_rows=[])
        context = self._make_context(
            settings_flow={
                "mode": "account_create",
                "step": "waiting_for_account_confirmation",
                "account": {
                    "name": "Monobank",
                    "currency": "USD",
                    "account_type": "main",
                    "initial_balance": Decimal("123.45"),
                },
            }
        )
        update, message, query = self._make_update("", "settings:acct:confirm:create")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_accounts_settings", new=AsyncMock()) as show_accounts_mock,
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        self.assertGreaterEqual(len(conn.fetchrow_calls), 2)
        insert_query, insert_args = conn.fetchrow_calls[-1]
        self.assertIn("INSERT INTO accounts", insert_query)
        self.assertIsNone(insert_args[1])
        self.assertEqual(insert_args[2], "Monobank")
        self.assertEqual(insert_args[3], "USD")
        self.assertEqual(insert_args[4], "main")
        self.assertEqual(insert_args[5], Decimal("123.45"))
        self.assertIsNone(insert_args[6])
        self.assertIsNone(insert_args[7])
        self.assertIsNone(insert_args[8])
        self.assertIsNone(insert_args[9])
        self.assertIsNone(insert_args[10])
        self.assertEqual(insert_args[11], "main")
        self.assertEqual(len(insert_args), 12)
        show_accounts_mock.assert_awaited_once()
        self.assertEqual(context.user_data["settings_flow"]["mode"], "account_create")
        self.assertEqual(context.user_data["settings_flow"]["account"]["name"], "Monobank")

    async def test_savings_account_create_persists_goal_fields(self) -> None:
        conn = DummyConn(fetch_rows=[])
        context = self._make_context(
            settings_flow={
                "mode": "account_create",
                "step": "waiting_for_account_confirmation",
                "account": {
                    "name": "На подорож",
                    "currency": "UAH",
                    "account_type": "savings",
                    "initial_balance": Decimal("0"),
                    "goal_amount": Decimal("10000"),
                    "goal_date": datetime(2026, 12, 31).date(),
                },
            }
        )
        update, _message, query = self._make_update("", "settings:acct:confirm:create")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_accounts_settings", new=AsyncMock()),
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        insert_query, insert_args = conn.fetchrow_calls[-1]
        self.assertIn("INSERT INTO accounts", insert_query)
        self.assertEqual(insert_args[4], "savings")
        self.assertEqual(insert_args[7], Decimal("10000"))
        self.assertEqual(insert_args[8], datetime(2026, 12, 31).date())
        self.assertEqual(insert_args[11], "savings")

    async def test_account_create_rejects_duplicate_name_even_with_different_currency(self) -> None:
        conn = DummyConn(fetch_rows=[{"label": "Monobank", "currency": "UAH"}])
        context = self._make_context(
            settings_flow={
                "mode": "account_create",
                "step": "waiting_for_account_confirmation",
                "account": {
                    "name": "monobank",
                    "currency": "USD",
                    "account_type": "main",
                    "initial_balance": Decimal("0"),
                },
            }
        )
        update, message, query = self._make_update("", "settings:acct:confirm:create")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(conn.execute_calls, [])
        self.assertIn("Рахунок з такою назвою вже існує", message.replies[0]["text"])
        flow = context.user_data["settings_flow"]
        self.assertEqual(flow["step"], bot_main._new_account_create_flow()["step"])
        self.assertNotIn("name", flow["account"])
        self.assertEqual(flow["account"]["currency"], "USD")
        self.assertEqual(flow["account"]["account_type"], "main")

    async def test_account_create_accepts_custom_currency_text(self) -> None:
        context = self._make_context(
            settings_flow={
                "mode": "account_create",
                "step": "waiting_for_account_currency",
                "account": {
                    "name": "Binance",
                    "account_type": "investment",
                },
            }
        )
        update, message = self._make_update("BTC")

        with patch.object(bot_main, "_ask_account_create_initial_balance", new=AsyncMock()) as ask_balance_mock:
            await bot_main._handle_account_create_text(update, context)

        self.assertEqual(context.user_data["settings_flow"]["account"]["currency"], "BTC")
        ask_balance_mock.assert_awaited_once_with(message, context)

    async def test_account_create_rejects_invalid_custom_currency_text(self) -> None:
        context = self._make_context(
            settings_flow={
                "mode": "account_create",
                "step": "waiting_for_account_currency",
                "account": {
                    "name": "Binance",
                    "account_type": "investment",
                },
            }
        )
        update, message = self._make_update("USDT1")

        with patch.object(bot_main, "_ask_account_create_initial_balance", new=AsyncMock()) as ask_balance_mock:
            await bot_main._handle_account_create_text(update, context)

        self.assertNotIn("currency", context.user_data["settings_flow"]["account"])
        ask_balance_mock.assert_not_awaited()
        self.assertGreaterEqual(len(message.replies), 1)

    async def test_transfer_target_selection_collects_state_before_db_write(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                1: {"id": 1, "label": "Cash", "currency": "UAH", "balance": Decimal("100")},
                2: {"id": 2, "label": "Card", "currency": "USD", "balance": Decimal("50")},
            }
        )
        context = self._make_context(
            transfer_flow={"source_account_id": 1, "step": "choose_target_account"}
        )
        update, message, query = self._make_update("", "transfer:target:2")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_transfer_rate_step", new=AsyncMock()),
            patch.object(bot_main, "_show_transfer_amount_step", new=AsyncMock()),
        ):
            await bot_main.transfer_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(conn.execute_calls, [])
        account_fetches = [call for call in conn.fetchrow_calls if len(call[1]) > 1]
        self.assertEqual(account_fetches[-2][1][1], 1)
        self.assertEqual(account_fetches[-1][1][1], 2)
        flow = context.user_data["transfer_flow"]
        self.assertEqual(flow["source_account_id"], 1)
        self.assertEqual(flow["target_account_id"], 2)
        self.assertEqual(flow["step"], "enter_exchange_rate")

    async def test_transfer_auto_rate_button_redirects_to_manual_rate_entry(self) -> None:
        conn = DummyConn()
        context = self._make_context(
            transfer_flow={
                "step": "enter_exchange_rate",
                "source_account_id": 1,
                "target_account_id": 2,
                "source_currency": "USD",
                "target_currency": "USDT",
            }
        )
        update, _message, query = self._make_update("", "transfer:rate:auto")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_transfer_rate_step", new=AsyncMock()) as show_rate_mock,
        ):
            await bot_main.transfer_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(context.user_data["transfer_flow"]["step"], "enter_exchange_rate")
        show_rate_mock.assert_awaited_once()

    async def test_transfer_cancel_writes_nothing(self) -> None:
        conn = DummyConn()
        context = self._make_context(
            transfer_flow={
                "step": bot_main.TX_FLOW_STEP_CONFIRMATION,
                "source_account_id": 1,
                "target_account_id": 2,
            }
        )
        update, message, query = self._make_update("", "transfer:cancel")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_accounts_settings", new=AsyncMock()),
        ):
            await bot_main.transfer_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(conn.execute_calls, [])
        self.assertNotIn("transfer_flow", context.user_data)

    async def _confirm_transfer(
        self,
        *,
        source_currency: str,
        target_currency: str,
        source_amount: Decimal,
        source_balance: Decimal,
        target_balance: Decimal,
        rate: Decimal | None,
        rate_source: str | None,
        source_account_id: int = 1,
        target_account_id: int = 2,
    ):
        conn = DummyConn(
            fetch_rows=[
                {
                    "id": source_account_id,
                    "label": "Source",
                    "currency": source_currency,
                    "balance": source_balance,
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
                {
                    "id": target_account_id,
                    "label": "Target",
                    "currency": target_currency,
                    "balance": target_balance,
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
            ]
        )
        context = self._make_context(
            transfer_flow={
                "step": bot_main.TX_FLOW_STEP_CONFIRMATION,
                "source_account_id": source_account_id,
                "target_account_id": target_account_id,
                "source_currency": source_currency,
                "target_currency": target_currency,
                "source_amount": source_amount,
                "fx_rate": rate,
                "rate_source": rate_source,
            }
        )
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["transfer_flow"], "transfer:confirm"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.transfer_callback(update, context)

        return conn, context, message, query

    async def test_transfer_confirm_requests_credit_limit_before_first_negative(self) -> None:
        conn, context, message, query = await self._confirm_transfer(
            source_currency="UAH",
            target_currency="UAH",
            source_amount=Decimal("150"),
            source_balance=Decimal("100"),
            target_balance=Decimal("50"),
            rate=None,
            rate_source=None,
        )

        self.assertTrue(query.answered)
        self.assertEqual(context.user_data["transfer_flow"]["step"], "await_credit_limit")
        self.assertEqual(context.user_data["transfer_flow"]["projected_balance"], Decimal("-50.00"))
        self.assertIn("кредитний режим", message.replies[-1]["text"].lower())
        self.assertEqual(conn.execute_calls, [])

    async def test_transfer_confirm_same_currency_updates_balances_and_transaction(self) -> None:
        conn, context, message, query = await self._confirm_transfer(
            source_currency="UAH",
            target_currency="UAH",
            source_amount=Decimal("25"),
            source_balance=Decimal("100"),
            target_balance=Decimal("50"),
            rate=None,
            rate_source=None,
        )

        self.assertTrue(query.answered)
        self.assertNotIn("transfer_flow", context.user_data)
        self.assertEqual(conn.execute_calls[0][1][2], Decimal("75.00"))
        self.assertEqual(conn.execute_calls[1][1][2], Decimal("75.00"))
        insert_args = conn.execute_calls[2][1]
        self.assertEqual(insert_args[3], Decimal("25.00"))
        self.assertEqual(insert_args[5], Decimal("25.00"))
        self.assertIsNone(insert_args[7])
        self.assertIsNone(insert_args[8])
        self.assertEqual(insert_args[11], "transfer")
        self.assertEqual(message.replies[-1]["text"].startswith("<b>✅ Переказ виконано</b>"), True)

    async def test_transfer_confirm_uah_to_usd_updates_balances_and_transaction(self) -> None:
        conn, context, message, query = await self._confirm_transfer(
            source_currency="UAH",
            target_currency="USD",
            source_amount=Decimal("400"),
            source_balance=Decimal("1000"),
            target_balance=Decimal("10"),
            rate=Decimal("40"),
            rate_source="manual",
        )

        self.assertTrue(query.answered)
        self.assertNotIn("transfer_flow", context.user_data)
        self.assertEqual(conn.execute_calls[0][1][2], Decimal("600.00"))
        self.assertEqual(conn.execute_calls[1][1][2], Decimal("20.00"))
        insert_args = conn.execute_calls[2][1]
        self.assertEqual(insert_args[3], Decimal("400.00"))
        self.assertEqual(insert_args[5], Decimal("10.00"))
        self.assertEqual(insert_args[7], Decimal("40"))
        self.assertEqual(insert_args[8], "1 USD = <b>40 UAH</b>")
        self.assertEqual(insert_args[9], "manual")
        self.assertEqual(insert_args[11], "transfer")
        self.assertEqual(message.replies[-1]["text"].startswith("<b>✅ Переказ виконано</b>"), True)

    async def test_transfer_confirm_usd_to_uah_updates_balances_and_transaction(self) -> None:
        conn, context, message, query = await self._confirm_transfer(
            source_currency="USD",
            target_currency="UAH",
            source_amount=Decimal("5"),
            source_balance=Decimal("25"),
            target_balance=Decimal("1000"),
            rate=Decimal("40"),
            rate_source="manual",
        )

        self.assertTrue(query.answered)
        self.assertNotIn("transfer_flow", context.user_data)
        self.assertEqual(conn.execute_calls[0][1][2], Decimal("20.00"))
        self.assertEqual(conn.execute_calls[1][1][2], Decimal("1200.00"))
        insert_args = conn.execute_calls[2][1]
        self.assertEqual(insert_args[3], Decimal("5.00"))
        self.assertEqual(insert_args[5], Decimal("200.00"))
        self.assertEqual(insert_args[7], Decimal("40"))
        self.assertEqual(insert_args[8], "1 USD = <b>40 UAH</b>")
        self.assertEqual(insert_args[9], "manual")
        self.assertEqual(insert_args[11], "transfer")
        self.assertEqual(message.replies[-1]["text"].startswith("<b>✅ Переказ виконано</b>"), True)

    async def test_transfer_double_confirm_does_not_double_write(self) -> None:
        conn = DummyConn(
            fetch_rows=[
                {
                    "id": 1,
                    "label": "Source",
                    "currency": "UAH",
                    "balance": Decimal("100"),
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
                {
                    "id": 2,
                    "label": "Target",
                    "currency": "UAH",
                    "balance": Decimal("50"),
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
            ]
        )
        context = self._make_context(
            transfer_flow={
                "step": bot_main.TX_FLOW_STEP_CONFIRMATION,
                "source_account_id": 1,
                "target_account_id": 2,
                "source_currency": "UAH",
                "target_currency": "UAH",
                "source_amount": Decimal("25"),
                "fx_rate": None,
                "rate_source": None,
            }
        )
        update, message, query = self._make_update("", prepare_confirmation(context.user_data["transfer_flow"], "transfer:confirm"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_show_accounts_settings", new=AsyncMock()),
        ):
            await bot_main.transfer_callback(update, context)
            await bot_main.transfer_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(len(conn.execute_calls), 3)
        self.assertNotIn("transfer_flow", context.user_data)
        self.assertGreaterEqual(len(message.replies), 1)


if __name__ == "__main__":
    unittest.main()
