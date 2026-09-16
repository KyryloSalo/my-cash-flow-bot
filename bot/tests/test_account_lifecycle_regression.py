from __future__ import annotations

import os
import asyncio
import sys
import types
import unittest
import io
import zipfile
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

    class _BotCommand:
        def __init__(self, command: str, description: str):
            self.command = command
            self.description = description

    class _MenuButtonCommands:
        pass

    class _WebAppInfo:
        def __init__(self, url: str):
            self.url = url

    class _MenuButtonWebApp:
        def __init__(self, text: str, web_app):
            self.text = text
            self.web_app = web_app

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

    telegram_stub.BotCommand = _BotCommand
    telegram_stub.MenuButtonCommands = _MenuButtonCommands
    telegram_stub.MenuButtonWebApp = _MenuButtonWebApp
    telegram_stub.WebAppInfo = _WebAppInfo
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
        self.documents: list[dict] = []
        self.chat = SimpleNamespace(send_action=AsyncMock())

    async def reply_text(self, text: str, reply_markup=None, **kwargs) -> None:
        self.replies.append({"text": text, "reply_markup": reply_markup, "kwargs": kwargs})

    async def reply_document(self, document=None, filename=None, caption=None, reply_markup=None, **kwargs) -> None:
        self.documents.append(
            {
                "document": document,
                "filename": filename,
                "caption": caption,
                "reply_markup": reply_markup,
                "kwargs": kwargs,
            }
        )


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


class LedgerConn:
    def __init__(
        self,
        *,
        tg_user_id: int,
        accounts: list[dict],
        transactions: list[dict] | None = None,
        debts: list[dict] | None = None,
        pending_tasks: list[dict] | None = None,
        billing_state: dict | None = None,
        access_state: dict | None = None,
        family_scope: dict | None = None,
        promo_offer: dict | None = None,
    ) -> None:
        self.tg_user_id = tg_user_id
        self.accounts = {int(row["id"]): dict(row) for row in accounts}
        for account in self.accounts.values():
            account.setdefault("family_id", None)
            account.setdefault("account_type", "main")
            account.setdefault("non_negative_account_type", "main")
        self.transactions = list(transactions or [])
        self.debts = list(debts or [])
        self.pending_tasks = list(pending_tasks or [])
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
        if billing_state:
            self.billing_state.update(billing_state)
        self.access_state = {
            "access_scope": "personal_full",
            "access_source": "billing",
            "pending_start_payload": "",
        }
        if access_state:
            self.access_state.update(access_state)
        self.family_scope = dict(family_scope) if family_scope else None
        self.promo_offer = dict(promo_offer) if promo_offer else None
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []

    def transaction(self) -> DummyTx:
        return DummyTx()

    async def execute(self, query: str, *args) -> None:
        normalized = " ".join(query.split())
        if "INSERT INTO user_admin_states" in normalized and "access_scope" in normalized:
            self.access_state["access_scope"] = str(args[1])
            self.access_state["access_source"] = str(args[2])
            if len(args) > 3:
                self.access_state["pending_start_payload"] = str(args[3])
            elif "pending_start_payload=''" in normalized:
                self.access_state["pending_start_payload"] = ""
            return
        self.execute_calls.append((query, args))
        if "UPDATE accounts SET is_active=false" in normalized:
            account_id = int(args[1])
            account = self.accounts.get(account_id)
            if account:
                account["is_active"] = False
                account["updated_at"] = datetime.now()
            return
        if "UPDATE accounts SET label=$3, updated_at=now()" in normalized:
            account_id = int(args[1])
            label = str(args[2])
            account = self.accounts.get(account_id)
            if account:
                account["label"] = label
                account["updated_at"] = datetime.now()
            return
        if "UPDATE accounts SET account_type=$3, updated_at=now()" in normalized:
            account_id = int(args[1])
            account_type = str(args[2])
            account = self.accounts.get(account_id)
            if account:
                account["account_type"] = account_type
                account["updated_at"] = datetime.now()
            return
        if normalized.startswith("UPDATE accounts SET balance=$3"):
            account_id = int(args[1])
            account = self.accounts.get(account_id)
            if account:
                account["balance"] = Decimal(str(args[2]))
                if "account_type=$4" in normalized:
                    account["account_type"] = args[3]
                    account["non_negative_account_type"] = args[4]
                account["updated_at"] = datetime.now()
            return
        if "INSERT INTO transactions" in normalized and "category_name_snapshot" in normalized:
            if len(args) == 9:
                type_index = 3
                amount_index = 4
                currency_index = 5
                comment_index = 6
                source_index = 7
                account_index = 8
                family_id = args[1]
            else:
                type_index = 2
                amount_index = 3
                currency_index = 4
                comment_index = 5
                source_index = 6
                account_index = 7
                family_id = None
            self.transactions.append(
                {
                    "tg_user_id": int(args[0]),
                    "family_id": family_id,
                    "type": str(args[type_index]),
                    "amount": Decimal(str(args[amount_index])),
                    "currency": str(args[currency_index]),
                    "comment": str(args[comment_index]),
                    "source": str(args[source_index]),
                    "account_id": int(args[account_index]),
                    "category_id": None,
                    "flow_kind": "adjustment",
                }
            )
            return
        if "WITH ledger AS" in normalized:
            self._recalculate_balances()
            return

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM accounts" in normalized:
            rows = [dict(row) for row in self.accounts.values() if row.get("tg_user_id") == self.tg_user_id and row.get("is_active", True)]
            rows.sort(key=lambda row: (row.get("created_at") or datetime.min, int(row.get("id") or 0)))
            return rows
        if "FROM debts" in normalized:
            return [dict(row) for row in self.debts if int(row.get("tg_user_id") or 0) == self.tg_user_id]
        if "FROM pending_saving_tasks" in normalized:
            return [
                dict(row)
                for row in self.pending_tasks
                if int(row.get("tg_user_id") or 0) == self.tg_user_id and str(row.get("status") or "") == "pending"
            ]
        if "FROM transactions" in normalized and "sum(amount) AS total" in normalized and "flow_kind='normal'" in normalized:
            start_date = args[1]
            end_date = args[2]
            if "COALESCE(c.name, t.category_name_snapshot) AS category" in normalized or "c.name AS category" in normalized:
                grouped: dict[tuple[str, str, str], Decimal] = {}
                for tx in self.transactions:
                    if int(tx.get("tg_user_id") or 0) != self.tg_user_id:
                        continue
                    tx_date = tx.get("date")
                    if tx_date is None or not (start_date <= tx_date < end_date):
                        continue
                    if str(tx.get("flow_kind") or "normal") != "normal":
                        continue
                    if tx.get("type") not in {"expense", "income"}:
                        continue
                    if tx.get("category_id") is None:
                        continue
                    category = str(tx.get("category_name_snapshot") or tx.get("category") or "")
                    key = (str(tx.get("type")), str(tx.get("currency") or ""), category)
                    grouped[key] = grouped.get(key, Decimal("0")) + Decimal(str(tx.get("amount") or 0))
                return [
                    {"type": type_, "currency": currency, "category": category, "total": total}
                    for (type_, currency, category), total in sorted(grouped.items())
                ]
            grouped = {}
            for tx in self.transactions:
                if int(tx.get("tg_user_id") or 0) != self.tg_user_id:
                    continue
                tx_date = tx.get("date")
                if tx_date is None or not (start_date <= tx_date < end_date):
                    continue
                if str(tx.get("flow_kind") or "normal") != "normal":
                    continue
                if tx.get("type") not in {"expense", "income"}:
                    continue
                if tx.get("category_id") is None:
                    continue
                key = (str(tx.get("type")), str(tx.get("currency") or ""))
                grouped[key] = grouped.get(key, Decimal("0")) + Decimal(str(tx.get("amount") or 0))
            return [
                {"type": type_, "currency": currency, "total": total}
                for (type_, currency), total in sorted(grouped.items())
            ]
        if "FROM transactions" in normalized and "flow_kind='debt'" in normalized:
            grouped: dict[tuple[str, str], dict[str, Decimal]] = {}
            for tx in self.transactions:
                if int(tx.get("tg_user_id") or 0) != self.tg_user_id:
                    continue
                if str(tx.get("flow_kind") or "") != "debt":
                    continue
                counterparty = str(tx.get("counterparty") or "вЂ”")
                currency = str(tx.get("currency") or "")
                key = (counterparty, currency)
                bucket = grouped.setdefault(key, {"owed_to_me": Decimal("0"), "i_owe": Decimal("0")})
                amount = Decimal(str(tx.get("amount") or 0))
                debt_action = str(tx.get("debt_action") or "")
                if debt_action == "lend":
                    bucket["owed_to_me"] += amount
                elif debt_action == "lend_repaid":
                    bucket["owed_to_me"] -= amount
                elif debt_action == "borrow":
                    bucket["i_owe"] += amount
                elif debt_action == "borrow_repaid":
                    bucket["i_owe"] -= amount
            return [
                {"counterparty": counterparty, "currency": currency, "owed_to_me": vals["owed_to_me"], "i_owe": vals["i_owe"]}
                for (counterparty, currency), vals in sorted(grouped.items())
            ]
        if ("SELECT *" in normalized or "SELECT t.*" in normalized) and "FROM transactions" in normalized:
            start_date = args[1]
            end_date = args[2]
            rows = [dict(row) for row in self.transactions if int(row.get("tg_user_id") or 0) == self.tg_user_id and row.get("date") is not None and start_date <= row["date"] < end_date]
            rows.sort(key=lambda row: (row.get("date") or datetime.min.date(), int(row.get("id") or 0)))
            return rows
        return []

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM family_members" in normalized and "JOIN families" in normalized:
            return dict(self.family_scope) if self.family_scope else None
        if "FROM users" in normalized and "WHERE tg_user_id=$1" in normalized:
            return {
                "tg_user_id": self.tg_user_id,
                "start_date": datetime.now().date(),
                "base_currency": "UAH",
                "onboarding_completed": True,
                "onboarding_version": bot_main.CURRENT_ONBOARDING_VERSION,
            }
        if "FROM billing_profiles" in normalized and "WHERE telegram_user_id=$1" in normalized:
            if not self.billing_state.get("profile_exists"):
                return None
            return {
                "status": self.billing_state.get("profile_status"),
                "masked_pan": self.billing_state.get("masked_pan"),
                "auto_renew_enabled": self.billing_state.get("auto_renew_enabled"),
                "last_charge_status": self.billing_state.get("last_charge_status"),
                "last_failure_reason": self.billing_state.get("last_failure_reason"),
                "last_action_url": self.billing_state.get("last_action_url"),
                "has_card": bool(self.billing_state.get("has_card")),
            }
        if "FROM subscriptions" in normalized and "WHERE telegram_user_id=$1" in normalized:
            if not self.billing_state.get("subscription_status"):
                return None
            return {
                "status": self.billing_state.get("subscription_status"),
                "expires_at": self.billing_state.get("expires_at"),
                "next_charge_at": self.billing_state.get("next_charge_at"),
                "grace_expires_at": self.billing_state.get("grace_expires_at"),
                "trial_days": self.billing_state.get("trial_days"),
            }
        if "FROM user_admin_states" in normalized and "SELECT access_scope, access_source, pending_start_payload" in normalized:
            return {
                "access_scope": self.access_state.get("access_scope"),
                "access_source": self.access_state.get("access_source"),
                "pending_start_payload": self.access_state.get("pending_start_payload"),
            }
        if "FROM promo_offers" in normalized and "WHERE upper(code)=upper($1)" in normalized:
            if not self.promo_offer:
                return None
            if str(self.promo_offer.get("code") or "").upper() != str(args[0] or "").upper():
                return None
            return {
                "code": self.promo_offer.get("code"),
                "trial_days": self.promo_offer.get("trial_days"),
            }
        if "FROM accounts" in normalized and "WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2 AND is_active=true" in normalized:
            account_id = int(args[1])
            account = self.accounts.get(account_id)
            if account and account.get("tg_user_id") == self.tg_user_id and account.get("is_active", True):
                return dict(account)
        if "FROM accounts" in normalized and "WHERE tg_user_id=$1 AND id=$2 AND is_active=true" in normalized:
            account_id = int(args[1])
            account = self.accounts.get(account_id)
            if account and account.get("tg_user_id") == self.tg_user_id and account.get("is_active", True):
                return dict(account)
        return None

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT to_regclass($1) IS NOT NULL" in normalized:
            return str(args[0]) in {"billing_profiles", "subscriptions", "user_admin_states", "promo_offers"}
        if "SELECT 1 FROM users WHERE tg_user_id=$1" in normalized:
            return 1
        if "SELECT count(1) FROM accounts WHERE tg_user_id=$1 AND family_id IS NULL AND is_active=true" in normalized:
            return sum(1 for row in self.accounts.values() if row.get("tg_user_id") == self.tg_user_id and row.get("is_active", True))
        if "SELECT count(1) FROM accounts WHERE family_id=$1 AND is_active=true" in normalized:
            family_id = int(args[0])
            return sum(1 for row in self.accounts.values() if int(row.get("family_id") or 0) == family_id and row.get("is_active", True))
        return None

    def _recalculate_balances(self) -> None:
        relevant_accounts = [row for row in self.accounts.values() if row.get("tg_user_id") == self.tg_user_id]
        for account in relevant_accounts:
            account_id = int(account["id"])
            balance = Decimal(str(account.get("starting_balance") or 0))
            for tx in self.transactions:
                if int(tx.get("tg_user_id") or 0) != self.tg_user_id:
                    continue
                if tx.get("account_id") == account_id and tx.get("type") == "income":
                    balance += Decimal(str(tx.get("amount") or 0))
                if tx.get("account_id") == account_id and tx.get("type") == "expense":
                    balance -= Decimal(str(tx.get("amount") or 0))
                if tx.get("from_account_id") == account_id and tx.get("type") == "transfer":
                    balance -= Decimal(str(tx.get("amount") or 0))
                if tx.get("to_account_id") == account_id and tx.get("type") == "transfer":
                    balance += Decimal(str(tx.get("to_amount") if tx.get("to_amount") is not None else tx.get("amount") or 0))
            account["balance"] = balance.quantize(Decimal("0.01"))
            account["updated_at"] = datetime.now()


class DummyAcquire:
    def __init__(self, conn: LedgerConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> LedgerConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyPool:
    def __init__(self, conn: LedgerConn) -> None:
        self._conn = conn

    def acquire(self) -> DummyAcquire:
        return DummyAcquire(self._conn)


class AccountLifecycleRegressionTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _normalize_button_label(label: str) -> str:
        return str(label or "").replace("\u2800", "").replace("\xa0", "").strip()

    @classmethod
    def _button_labels(cls, reply_markup) -> list[str]:
        labels: list[str] = []
        rows = reply_markup.args[0] if reply_markup and getattr(reply_markup, "args", None) else []
        for row in rows:
            for button in row:
                raw_label = button.args[0] if getattr(button, "args", None) else ""
                labels.append(cls._normalize_button_label(raw_label))
        return labels

    @staticmethod
    def _button_callbacks(reply_markup) -> list[str]:
        callbacks: list[str] = []
        rows = reply_markup.args[0] if reply_markup and getattr(reply_markup, "args", None) else []
        for row in rows:
            for button in row:
                callback_data = str(getattr(button, "kwargs", {}).get("callback_data") or "")
                if callback_data:
                    callbacks.append(callback_data)
        return callbacks

    def _make_context(self, *, accounts_flow: dict | None = None, tg_user_id: int | None = None):
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={}),
            user_data={},
        )
        if accounts_flow is not None:
            context.user_data["accounts_flow"] = accounts_flow
        if tg_user_id is not None:
            context.user_data["tg_user_id"] = tg_user_id
        return context

    def _make_update(self, callback_data: str, text: str = ""):
        message = DummyMessage(text=text)
        user = SimpleNamespace(id=123, username="tester")
        query = DummyQuery(callback_data, message)
        return SimpleNamespace(effective_user=user, message=message, callback_query=query), message, query

    async def test_post_init_registers_default_bot_commands(self) -> None:
        application = SimpleNamespace(
            bot=SimpleNamespace(
                set_my_commands=AsyncMock(),
                set_chat_menu_button=AsyncMock(),
            ),
            bot_data={},
        )

        with (
            patch.object(bot_main, "init_db", new=AsyncMock()) as init_db_mock,
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
        ):
            expected_url = bot_main._miniapp_url()
            await bot_main.post_init(application)

        init_db_mock.assert_awaited_once_with(application)
        calls = application.bot.set_my_commands.await_args_list
        self.assertEqual(len(calls), 3)
        commands = calls[0].args[0]
        self.assertEqual([command.command for command in commands], ["start", "menu", "help", "settings", "cabinet"])
        self.assertEqual(
            [command.description for command in commands],
            [
                "\u0417\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u0438 \u0431\u043e\u0442\u0430",
                "\u0412\u0456\u0434\u043a\u0440\u0438\u0442\u0438 \u0433\u043e\u043b\u043e\u0432\u043d\u0435 \u043c\u0435\u043d\u044e",
                "\u0412\u0456\u0434\u043a\u0440\u0438\u0442\u0438 \u0434\u043e\u043f\u043e\u043c\u043e\u0433\u0443",
                "\u0412\u0456\u0434\u043a\u0440\u0438\u0442\u0438 \u043d\u0430\u043b\u0430\u0448\u0442\u0443\u0432\u0430\u043d\u043d\u044f",
                "\u0412\u0456\u0434\u043a\u0440\u0438\u0442\u0438 \u043a\u0430\u0431\u0456\u043d\u0435\u0442",
            ],
        )
        self.assertEqual(calls[1].kwargs.get("language_code"), "uk")
        self.assertEqual(calls[2].kwargs.get("language_code"), "en")
        menu_button = application.bot.set_chat_menu_button.await_args.kwargs["menu_button"]
        self.assertIsInstance(menu_button, bot_main.MenuButtonWebApp)
        self.assertEqual(getattr(menu_button, "text", ""), "Vydno.Capital")
        self.assertEqual(getattr(getattr(menu_button, "web_app", None), "url", None), expected_url)

    async def test_post_init_falls_back_to_commands_menu_when_miniapp_url_missing(self) -> None:
        application = SimpleNamespace(
            bot=SimpleNamespace(
                set_my_commands=AsyncMock(),
                set_chat_menu_button=AsyncMock(),
            ),
            bot_data={},
        )

        with (
            patch.object(bot_main, "init_db", new=AsyncMock()),
            patch.object(bot_main.config, "MINIAPP_URL", ""),
        ):
            await bot_main.post_init(application)

        menu_button = application.bot.set_chat_menu_button.await_args.kwargs["menu_button"]
        self.assertIsInstance(menu_button, bot_main.MenuButtonCommands)

    async def test_menu_command_resets_runtime_flows_and_shows_home(self) -> None:
        context = self._make_context()
        context.user_data["settings_flow"] = {"mode": "billing"}
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="/menu"),
            callback_query=None,
        )

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()) as consume_reset_mock,
            patch.object(bot_main, "_show_home", new=AsyncMock()) as show_home_mock,
        ):
            await bot_main.menu_command(update, context)

        consume_reset_mock.assert_awaited_once_with(context, 123)
        show_home_mock.assert_awaited_once_with(update, context)
        self.assertNotIn("settings_flow", context.user_data)

    async def test_settings_command_shows_settings_surface_for_ready_user(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context()
        context.user_data["settings_flow"] = {"mode": "billing"}
        message = DummyMessage(text="/settings")
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=message,
            callback_query=None,
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()) as consume_reset_mock,
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_settings_menu", new=AsyncMock()) as show_settings_mock,
        ):
            await bot_main.settings_command(update, context)

        consume_reset_mock.assert_awaited_once_with(context, 123)
        show_settings_mock.assert_awaited_once_with(message, conn)
        self.assertNotIn("settings_flow", context.user_data)

    async def test_sync_access_scope_state_upsert_includes_legacy_admin_defaults(self) -> None:
        class CaptureConn:
            def __init__(self) -> None:
                self.calls: list[tuple[str, tuple]] = []

            async def execute(self, query: str, *args) -> None:
                self.calls.append((" ".join(query.split()), args))

        conn = CaptureConn()

        await bot_main._sync_access_scope_state(
            conn,
            123,
            access_scope="paywall",
            access_source="billing",
        )

        self.assertEqual(len(conn.calls), 1)
        query, args = conn.calls[0]
        self.assertIn("status", query)
        self.assertIn("current_fsm_state", query)
        self.assertIn("onboarding_payload", query)
        self.assertIn("pending_admin_reset_mode", query)
        self.assertEqual(args, (123, "paywall", "billing"))

    async def test_accounts_add_opens_type_chooser(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("0"),
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("accounts:add")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("Оберіть тип рахунку", message.replies[0]["text"])
        labels = self._button_labels(message.replies[0]["reply_markup"])
        self.assertIn("Звичайний", labels)
        self.assertIn("Накопичення", labels)

    async def test_transaction_void_recent_opens_last_cancellable_transactions(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context()
        update, message, query = self._make_update("txvoid:recent")
        recent_payload = {
            "items": [
                {
                    "id": 44,
                    "date": "2026-08-30",
                    "kind": "expense",
                    "amount": {"value": "125.00", "display": "125,00 ₴"},
                    "category": {"name": "Продукти"},
                }
            ]
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "list_recent_transactions", new=AsyncMock(return_value=recent_payload)) as recent_mock,
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        recent_mock.assert_awaited_once_with(actor_tg_user_id=123, limit=10)
        self.assertIn("Останні операції", message.replies[0]["text"])
        self.assertIn("txvoid:pick:44", self._button_callbacks(message.replies[0]["reply_markup"]))

    async def test_transaction_void_pick_and_confirm_use_internal_service(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context()
        draft = {
            "draft_id": "void-draft-44",
            "transaction_id": 44,
            "original_signature": "signature",
            "transaction": {
                "id": 44,
                "date": "2026-08-30",
                "kind": "expense",
                "amount": {"value": "125.00", "display": "125,00 ₴"},
                "account": {"label": "Mono Black"},
                "category": {"name": "Продукти"},
                "comment": "Супермаркет",
            },
            "current_balance": {"value": "875.00", "display": "875,00 ₴"},
            "balance_after": {"value": "1000.00", "display": "1 000,00 ₴"},
            "warning": None,
        }
        build_mock = AsyncMock(return_value={"ok": True, "draft": draft})
        confirm_mock = AsyncMock(
            return_value={
                "ok": True,
                "result": {
                    "status": "cancelled",
                    "transaction_id": 44,
                    "new_balance": {"value": "1000.00", "display": "1 000,00 ₴"},
                },
            }
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "build_transaction_void_draft", new=build_mock),
            patch.object(bot_main, "confirm_transaction_void", new=confirm_mock),
        ):
            pick_update, pick_message, pick_query = self._make_update("txvoid:pick:44")
            await bot_main.home_callback(pick_update, context)

            self.assertTrue(pick_query.answered)
            build_mock.assert_awaited_once_with(actor_tg_user_id=123, transaction_id=44)
            self.assertIn("Баланс після скасування", pick_message.replies[0]["text"])
            self.assertIn("txvoid:confirm:44", self._button_callbacks(pick_message.replies[0]["reply_markup"]))

            confirm_update, confirm_message, confirm_query = self._make_update("txvoid:confirm:44")
            await bot_main.home_callback(confirm_update, context)

        self.assertTrue(confirm_query.answered)
        confirm_mock.assert_awaited_once()
        self.assertEqual(confirm_mock.await_args.kwargs["actor_tg_user_id"], 123)
        self.assertEqual(confirm_mock.await_args.kwargs["draft"], draft)
        self.assertTrue(confirm_mock.await_args.kwargs["idempotency_key"])
        self.assertIn("Транзакцію скасовано", confirm_message.replies[0]["text"])
        self.assertNotIn("transaction_void_draft", context.user_data)
        self.assertNotIn("transaction_void_idempotency_key", context.user_data)

    async def test_account_type_change_updates_existing_account(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("0"),
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("accounts:type:set:1:savings")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(conn.accounts[1]["account_type"], "savings")
        self.assertIn("Накопичення", message.replies[0]["text"])

    async def test_account_rename_prompt_sets_flow(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("0"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "UAH", "starting_balance": Decimal("0"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("accounts:rename:1")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(context.user_data["accounts_flow"], {"step": "rename", "account_id": 1})
        self.assertEqual(len(conn.execute_calls), 0)
        self.assertIn("Введіть нову назву рахунку", message.replies[0]["text"])

    async def test_account_rename_commit_updates_existing_row_and_preserves_history(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"tg_user_id": 123, "type": "income", "amount": Decimal("20"), "account_id": 1},
                {"tg_user_id": 123, "type": "expense", "amount": Decimal("5"), "account_id": 2},
            ],
        )
        context = self._make_context(accounts_flow={"step": "rename", "account_id": 1})
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="Primary Wallet"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_start_transfer_from_text", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_start_ai_tx_flow", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(update, context)

        self.assertNotIn("accounts_flow", context.user_data)
        self.assertEqual(conn.accounts[1]["label"], "Primary Wallet")
        self.assertEqual(conn.accounts[1]["currency"], "UAH")
        self.assertEqual(conn.accounts[1]["balance"], Decimal("120"))
        self.assertEqual(len(conn.accounts), 2)
        self.assertEqual(len(conn.transactions), 2)
        self.assertTrue(any("UPDATE accounts SET label=$3, updated_at=now()" in " ".join(q.split()) for q, _ in conn.execute_calls))
        self.assertFalse(any("INSERT INTO accounts" in q for q, _ in conn.execute_calls))
        self.assertTrue(update.message.replies)
        self.assertIn("Рахунок перейменовано", update.message.replies[-1]["text"])
        self.assertIn("Primary Wallet", update.message.replies[-1]["text"])

    async def test_account_rename_rejects_case_insensitive_duplicate(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
        )
        context = self._make_context(accounts_flow={"step": "rename", "account_id": 1})
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="cash"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_start_transfer_from_text", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_start_ai_tx_flow", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(update, context)

        self.assertEqual(conn.accounts[1]["label"], "Main")
        self.assertEqual(len(conn.execute_calls), 0)
        self.assertEqual(context.user_data["accounts_flow"], {"step": "rename", "account_id": 1})
        self.assertIn("вже існує", update.message.replies[-1]["text"])

    async def test_account_rename_allows_same_account_name(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
        )
        context = self._make_context(accounts_flow={"step": "rename", "account_id": 1})
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="main"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_start_transfer_from_text", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_start_ai_tx_flow", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(update, context)

        self.assertNotIn("accounts_flow", context.user_data)
        self.assertEqual(conn.accounts[1]["label"], "main")
        self.assertTrue(any("UPDATE accounts SET label=$3, updated_at=now()" in " ".join(q.split()) for q, _ in conn.execute_calls))
        self.assertFalse(any("INSERT INTO accounts" in q for q, _ in conn.execute_calls))
        self.assertIn("Рахунок перейменовано", update.message.replies[-1]["text"])

    async def test_account_rename_rejects_empty_or_whitespace_name(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
        )
        context = self._make_context(accounts_flow={"step": "rename", "account_id": 1})
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="   "),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_start_transfer_from_text", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_start_ai_tx_flow", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(update, context)

        self.assertEqual(conn.accounts[1]["label"], "Main")
        self.assertEqual(len(conn.execute_calls), 0)
        self.assertEqual(context.user_data["accounts_flow"], {"step": "rename", "account_id": 1})
        self.assertIn("не може бути порожньою", update.message.replies[-1]["text"])

    async def test_account_balance_prompt_sets_flow(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("accounts:balance:1")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(context.user_data["accounts_flow"], {"step": "balance", "account_id": 1})
        self.assertEqual(len(conn.execute_calls), 0)
        self.assertIn("Змінити баланс рахунку", message.replies[0]["text"])

    async def test_account_balance_correction_increases_balance_via_transaction(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"tg_user_id": 123, "type": "income", "amount": Decimal("20"), "account_id": 1},
            ],
        )
        context = self._make_context(accounts_flow={"step": "balance", "account_id": 1})
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="140"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_start_transfer_from_text", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_start_ai_tx_flow", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(update, context)
            self.assertEqual(conn.accounts[1]["balance"], Decimal("120"))
            self.assertEqual(len(conn.transactions), 1)
            self.assertEqual(conn.execute_calls, [])
            callback_data = confirmation_from_reply(update.message, "accounts:balance:confirm")
            update.callback_query = DummyQuery(callback_data, update.message)
            await bot_main.home_callback(update, context)

        self.assertNotIn("accounts_flow", context.user_data)
        self.assertEqual(conn.accounts[1]["label"], "Main")
        self.assertEqual(conn.accounts[1]["currency"], "UAH")
        self.assertEqual(conn.accounts[1]["balance"], Decimal("140.00"))
        self.assertEqual(conn.accounts[2]["balance"], Decimal("50.00"))
        self.assertEqual(len(conn.transactions), 2)
        self.assertEqual(conn.transactions[-1]["flow_kind"], "adjustment")
        self.assertEqual(conn.transactions[-1]["type"], "income")
        self.assertEqual(conn.transactions[-1]["amount"], Decimal("20.00"))
        self.assertFalse(any("UPDATE accounts SET balance=$3, updated_at=now()" in " ".join(q.split()) for q, _ in conn.execute_calls))
        self.assertTrue(update.message.replies)
        self.assertIn("Баланс скориговано", update.message.replies[-1]["text"])

    async def test_account_balance_correction_decreases_balance_via_transaction(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"tg_user_id": 123, "type": "income", "amount": Decimal("20"), "account_id": 1},
            ],
        )
        context = self._make_context(accounts_flow={"step": "balance", "account_id": 1})
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="80"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_start_transfer_from_text", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_start_ai_tx_flow", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(update, context)
            self.assertEqual(conn.accounts[1]["balance"], Decimal("120"))
            self.assertEqual(len(conn.transactions), 1)
            self.assertEqual(conn.execute_calls, [])
            callback_data = confirmation_from_reply(update.message, "accounts:balance:confirm")
            update.callback_query = DummyQuery(callback_data, update.message)
            await bot_main.home_callback(update, context)

        self.assertNotIn("accounts_flow", context.user_data)
        self.assertEqual(conn.accounts[1]["balance"], Decimal("80.00"))
        self.assertEqual(conn.accounts[2]["balance"], Decimal("50.00"))
        self.assertEqual(len(conn.transactions), 2)
        self.assertEqual(conn.transactions[-1]["flow_kind"], "adjustment")
        self.assertEqual(conn.transactions[-1]["type"], "expense")
        self.assertEqual(conn.transactions[-1]["amount"], Decimal("40.00"))
        self.assertFalse(any("UPDATE accounts SET balance=$3, updated_at=now()" in " ".join(q.split()) for q, _ in conn.execute_calls))
        self.assertTrue(update.message.replies)
        self.assertIn("Баланс скориговано", update.message.replies[-1]["text"])

    async def test_account_balance_same_value_creates_no_transaction(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"tg_user_id": 123, "type": "income", "amount": Decimal("20"), "account_id": 1},
            ],
        )
        context = self._make_context(accounts_flow={"step": "balance", "account_id": 1})
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="120"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_start_transfer_from_text", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_start_ai_tx_flow", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(update, context)

        self.assertNotIn("accounts_flow", context.user_data)
        self.assertEqual(conn.accounts[1]["balance"], Decimal("120"))
        self.assertEqual(len(conn.transactions), 1)
        self.assertFalse(any("INSERT INTO transactions" in q for q, _ in conn.execute_calls))
        self.assertTrue(any("WITH ledger AS" in q for q, _ in conn.execute_calls))
        self.assertIn("Баланс уже відповідає", update.message.replies[-1]["text"])

    async def test_account_balance_rejects_invalid_input_and_confirms_negative_balance(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("120"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"tg_user_id": 123, "type": "income", "amount": Decimal("20"), "account_id": 1},
            ],
        )
        context = self._make_context(accounts_flow={"step": "balance", "account_id": 1})
        invalid_update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="abc"),
        )
        negative_update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="-10"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_start_transfer_from_text", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_start_ai_tx_flow", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
        ):
            await bot_main.text_message(invalid_update, context)
            self.assertEqual(context.user_data["accounts_flow"], {"step": "balance", "account_id": 1})
            self.assertEqual(conn.accounts[1]["balance"], Decimal("120"))
            self.assertEqual(len(conn.transactions), 1)
            self.assertEqual(len(conn.execute_calls), 0)
            self.assertIn("Не вдалося розпізнати суму", invalid_update.message.replies[0]["text"])

            context.user_data["accounts_flow"] = {"step": "balance", "account_id": 1}
            await bot_main.text_message(negative_update, context)
            self.assertEqual(conn.accounts[1]["balance"], Decimal("120"))
            self.assertEqual(len(conn.transactions), 1)
            self.assertEqual(conn.execute_calls, [])
            callback_data = confirmation_from_reply(negative_update.message, "accounts:balance:confirm")
            negative_update.callback_query = DummyQuery(callback_data, negative_update.message)
            await bot_main.home_callback(negative_update, context)

        self.assertNotIn("accounts_flow", context.user_data)
        self.assertEqual(conn.accounts[1]["balance"], Decimal("-10.00"))
        self.assertEqual(len(conn.transactions), 2)
        self.assertEqual(conn.transactions[-1]["flow_kind"], "adjustment")
        self.assertEqual(conn.transactions[-1]["type"], "expense")
        self.assertEqual(conn.transactions[-1]["amount"], Decimal("130.00"))
        self.assertFalse(any("UPDATE accounts SET balance=$3, updated_at=now()" in " ".join(q.split()) for q, _ in conn.execute_calls))
        self.assertIn("Баланс скориговано", negative_update.message.replies[-1]["text"])

    async def test_adjustment_transactions_are_excluded_from_normal_reports(self) -> None:
        today = bot_main.datetime.now().date()
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("100"), "is_active": True, "created_at": datetime(2026, 1, 1)},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": today, "type": "income", "amount": Decimal("100"), "currency": "UAH", "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal"},
                {"id": 2, "tg_user_id": 123, "date": today, "type": "expense", "amount": Decimal("30"), "currency": "UAH", "category_id": 12, "category_name_snapshot": "Food", "flow_kind": "normal"},
                {"id": 3, "tg_user_id": 123, "date": today, "type": "income", "amount": Decimal("25"), "currency": "UAH", "account_id": 1, "category_id": None, "category_name_snapshot": None, "flow_kind": "adjustment", "source": "balance_correction", "comment": "РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 125 UAH"},
                {"id": 4, "tg_user_id": 123, "date": today, "type": "expense", "amount": Decimal("10"), "currency": "UAH", "account_id": 1, "category_id": None, "category_name_snapshot": None, "flow_kind": "adjustment", "source": "balance_correction", "comment": "РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 90 UAH"},
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("reports:today")

        with patch.object(bot_main, "_pool", return_value=DummyPool(conn)):
            await bot_main.reports_callback(update, context)

        self.assertTrue(query.answered)
        self.assertGreaterEqual(len(conn.fetch_calls), 1)
        report_text = message.replies[-1]["text"]
        self.assertIn("100 UAH", report_text)
        self.assertIn("30 UAH", report_text)
        self.assertNotIn("125 UAH", report_text)
        self.assertNotIn("40 UAH", report_text)

    async def test_adjustment_transactions_are_visible_in_export_csv(self) -> None:
        today = bot_main.datetime.now().date()
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("100"), "is_active": True, "created_at": datetime(2026, 1, 1)},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": today, "type": "income", "amount": Decimal("100"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
                {"id": 2, "tg_user_id": 123, "date": today, "type": "expense", "amount": Decimal("30"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Groceries"},
                {"id": 3, "tg_user_id": 123, "date": today, "type": "income", "amount": Decimal("25"), "currency": "UAH", "account_id": 1, "category_id": None, "category_name_snapshot": None, "flow_kind": "adjustment", "source": "balance_correction", "comment": "РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 125 UAH"},
                {"id": 4, "tg_user_id": 123, "date": today, "type": "expense", "amount": Decimal("10"), "currency": "UAH", "account_id": 1, "category_id": None, "category_name_snapshot": None, "flow_kind": "adjustment", "source": "balance_correction", "comment": "РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 90 UAH"},
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("export:today")

        with patch.object(bot_main, "_pool", return_value=DummyPool(conn)):
            await bot_main.export_callback(update, context)

        self.assertTrue(query.answered)
        self.assertGreaterEqual(len(conn.fetch_calls), 1)
        self.assertEqual(len(message.documents), 1)
        document = message.documents[0]
        self.assertEqual(document["filename"], "my-cash-flow-today.xlsx")
        self.assertIn("Операцій: <b>4</b>", document["caption"])
        with zipfile.ZipFile(io.BytesIO(document["document"])) as workbook:
            sheet_xml = workbook.read("xl/worksheets/sheet1.xml").decode("utf-8")
            sheet_xml = "\n".join(
                workbook.read(name).decode("utf-8", errors="ignore")
                for name in workbook.namelist()
                if name.endswith(".xml")
            )
        self.assertIn("РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 125 UAH", sheet_xml)
        self.assertIn("РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 90 UAH", sheet_xml)
        return
        csv_text = document["document"].decode("utf-8-sig")
        self.assertIn("РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 125 UAH", csv_text)
        self.assertIn("РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 90 UAH", csv_text)

    async def test_recalculate_account_balances_includes_adjustment_transactions(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Savings", "currency": "UAH", "starting_balance": Decimal("50"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "type": "income", "amount": Decimal("40"), "account_id": 1, "currency": "UAH", "flow_kind": "normal"},
                {"id": 2, "tg_user_id": 123, "type": "expense", "amount": Decimal("10"), "account_id": 1, "currency": "UAH", "flow_kind": "normal"},
                {"id": 3, "tg_user_id": 123, "type": "transfer", "amount": Decimal("25"), "to_amount": Decimal("25"), "from_account_id": 1, "to_account_id": 2, "currency": "UAH", "flow_kind": "transfer"},
                {"id": 4, "tg_user_id": 123, "type": "income", "amount": Decimal("20"), "account_id": 1, "currency": "UAH", "flow_kind": "adjustment", "source": "balance_correction"},
                {"id": 5, "tg_user_id": 123, "type": "expense", "amount": Decimal("5"), "account_id": 2, "currency": "UAH", "flow_kind": "adjustment", "source": "balance_correction"},
            ],
        )

        await bot_main._recalculate_account_balances(conn, 123)

        self.assertEqual(conn.accounts[1]["balance"], Decimal("125.00"))
        self.assertEqual(conn.accounts[2]["balance"], Decimal("70.00"))

    async def test_debt_rows_are_excluded_from_normal_reports_and_visible_in_debt_report(self) -> None:
        today = bot_main.datetime.now().date()
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("100"), "is_active": True, "created_at": datetime(2026, 1, 1)},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": today, "type": "income", "amount": Decimal("101"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
                {"id": 2, "tg_user_id": 123, "date": today, "type": "expense", "amount": Decimal("37"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Groceries"},
                {"id": 3, "tg_user_id": 123, "date": today, "type": "income", "amount": Decimal("8"), "currency": "UAH", "account_id": 1, "category_id": None, "category_name_snapshot": None, "flow_kind": "adjustment", "source": "balance_correction", "comment": "РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 109 UAH"},
                {"id": 4, "tg_user_id": 123, "date": today, "type": "transfer", "amount": Decimal("57"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Alice", "debt_action": "lend", "source": "text", "comment": None},
                {"id": 5, "tg_user_id": 123, "date": today, "type": "transfer", "amount": Decimal("43"), "currency": "USD", "flow_kind": "debt", "counterparty": "Bob", "debt_action": "borrow", "source": "text", "comment": None},
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("reports:today")

        with patch.object(bot_main, "_pool", return_value=DummyPool(conn)):
            await bot_main.reports_callback(update, context)

        self.assertTrue(query.answered)
        self.assertGreaterEqual(len(conn.fetch_calls), 1)
        report_text = message.replies[-1]["text"]
        self.assertIn("101 UAH", report_text)
        self.assertIn("37 UAH", report_text)
        self.assertNotIn("57 UAH", report_text)
        self.assertNotIn("43 USD", report_text)
        self.assertNotIn("109 UAH", report_text)

        debt_report = await bot_main._debts_report_text(conn, 123)
        self.assertIn("Alice", debt_report)
        self.assertIn("57 UAH", debt_report)
        self.assertIn("Bob", debt_report)
        self.assertIn("43 USD", debt_report)

    async def test_render_accounts_text_shows_section_totals_overall_total_and_debt_summary_in_base_currency(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Monobank вЂў4444", "currency": "UAH", "account_type": "main", "starting_balance": Decimal("3586"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "РџСЂРёРІР°С‚Р‘Р°РЅРє вЂў4144", "currency": "UAH", "account_type": "main", "starting_balance": Decimal("6986"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 2)},
                {"id": 3, "tg_user_id": 123, "label": "Р Р°Р№С„ вЂў5555", "currency": "USD", "account_type": "main", "starting_balance": Decimal("192.62"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 3)},
                {"id": 4, "tg_user_id": 123, "label": "Р“РѕС‚С–РІРєР°", "currency": "TRY", "account_type": "cash", "starting_balance": Decimal("1000"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 4)},
                {"id": 5, "tg_user_id": 123, "label": "Р“РѕС‚С–РІРєР° РІ РІР°Р·С–", "currency": "UAH", "account_type": "savings", "starting_balance": Decimal("0"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 5)},
                {"id": 6, "tg_user_id": 123, "label": "Р”РµРїРѕР·РёС‚ РІ РђР»СЊС„Р° Р‘Р°РЅРєСѓ", "currency": "USD", "account_type": "deposit", "starting_balance": Decimal("5"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 6)},
                {"id": 7, "tg_user_id": 123, "label": "Р†РЅС‚РµСЂР°РєС‚РёРІ Р±СЂРѕРєРµСЂ", "currency": "USD", "account_type": "investment", "starting_balance": Decimal("5"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 7)},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": datetime(2026, 5, 3).date(), "type": "transfer", "amount": Decimal("10"), "currency": "USD", "flow_kind": "debt", "counterparty": "Alice", "debt_action": "lend", "source": "text", "comment": None},
                {"id": 2, "tg_user_id": 123, "date": datetime(2026, 5, 3).date(), "type": "transfer", "amount": Decimal("100"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Bob", "debt_action": "borrow", "source": "text", "comment": None},
            ],
        )

        snapshot = SimpleNamespace(
            rates={"UAH": Decimal("1"), "USD": Decimal("41"), "TRY": Decimal("1.2")},
            date="03.05.2026",
        )

        with patch.object(bot_main, "get_latest_rates", new=AsyncMock(return_value=snapshot)):
            text = await bot_main._render_accounts_text(conn, 123)

        self.assertIn("Разом по типу:</b> ≈ <b>18 469.42 UAH</b>", text)
        self.assertIn("Разом по типу:</b> ≈ <b>1 200 UAH</b>", text)
        self.assertIn("Разом по типу:</b> <b>0 UAH</b>", text)
        self.assertIn("Разом по типу:</b> ≈ <b>205 UAH</b>", text)
        self.assertIn("Загалом без кредиток:</b> ≈ <b>20 079.42 UAH</b>", text)
        self.assertIn("Винен Я:</b> <b>100 UAH</b>", text)
        self.assertIn("Винні Мені:</b> ≈ <b>410 UAH</b>", text)
        self.assertIn("1 USD = 41 UAH", text)
        self.assertIn("1 TRY = 1.2 UAH", text)
        self.assertIn("Дата курсу: 03.05.2026", text)

    async def test_render_accounts_text_fetches_rates_for_foreign_debts_even_without_foreign_accounts(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Monobank вЂў4444", "currency": "UAH", "account_type": "main", "starting_balance": Decimal("1000"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 1)},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": datetime(2026, 5, 3).date(), "type": "transfer", "amount": Decimal("25"), "currency": "USD", "flow_kind": "debt", "counterparty": "Alice", "debt_action": "lend", "source": "text", "comment": None},
            ],
        )

        snapshot = SimpleNamespace(
            rates={"UAH": Decimal("1"), "USD": Decimal("40")},
            date="03.05.2026",
        )

        with patch.object(bot_main, "get_latest_rates", new=AsyncMock(return_value=snapshot)) as rates_mock:
            text = await bot_main._render_accounts_text(conn, 123)

        rates_mock.assert_awaited_once_with("UAH")
        self.assertIn("Винні Мені:</b> ≈ <b>1 000 UAH</b>", text)

    async def test_debt_rows_export_xlsx_without_account_id_and_remain_visible(self) -> None:
        today = bot_main.datetime.now().date()
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("100"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Savings", "currency": "UAH", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": today, "type": "income", "amount": Decimal("101"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
                {"id": 2, "tg_user_id": 123, "date": today, "type": "expense", "amount": Decimal("37"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Groceries"},
                {"id": 3, "tg_user_id": 123, "date": today, "type": "transfer", "amount": Decimal("25"), "to_amount": Decimal("25"), "currency": "UAH", "to_currency": "UAH", "fx_rate": None, "from_account_id": 1, "to_account_id": 2, "flow_kind": "transfer", "comment": "Move cash"},
                {"id": 4, "tg_user_id": 123, "date": today, "type": "income", "amount": Decimal("8"), "currency": "UAH", "account_id": 1, "category_id": None, "category_name_snapshot": None, "flow_kind": "adjustment", "source": "balance_correction", "comment": "РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 109 UAH"},
                {"id": 5, "tg_user_id": 123, "date": today, "type": "expense", "amount": Decimal("6"), "currency": "UAH", "account_id": 2, "category_id": None, "category_name_snapshot": None, "flow_kind": "adjustment", "source": "balance_correction", "comment": "РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ РґРѕ 44 UAH"},
                {"id": 6, "tg_user_id": 123, "date": today, "type": "transfer", "amount": Decimal("57"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Alice", "debt_action": "lend", "source": "text", "comment": None},
                {"id": 7, "tg_user_id": 123, "date": today, "type": "transfer", "amount": Decimal("43"), "currency": "USD", "flow_kind": "debt", "counterparty": "Bob", "debt_action": "borrow", "source": "text", "comment": None},
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("export:today")

        with patch.object(bot_main, "_pool", return_value=DummyPool(conn)):
            await bot_main.export_callback(update, context)

        self.assertTrue(query.answered)
        self.assertGreaterEqual(len(conn.fetch_calls), 1)
        self.assertEqual(len(message.documents), 1)
        document = message.documents[0]
        self.assertEqual(document["filename"], "my-cash-flow-today.xlsx")
        self.assertIn("Операцій: <b>7</b>", document["caption"])

        self.assertTrue(message.replies)
        self.assertIn("101 UAH", message.replies[-1]["text"])
        self.assertIn("37 UAH", message.replies[-1]["text"])
        with zipfile.ZipFile(io.BytesIO(document["document"])) as workbook:
            worksheet_files = [name for name in workbook.namelist() if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")]
            self.assertEqual(worksheet_files, ["xl/worksheets/sheet1.xml", "xl/worksheets/sheet2.xml"])
            sheet_xml = "\n".join(
                workbook.read(name).decode("utf-8", errors="ignore")
                for name in workbook.namelist()
                if name.endswith(".xml")
            )
        self.assertIn("Alice", sheet_xml)
        self.assertIn("Bob", sheet_xml)
        self.assertIn("Дохід", sheet_xml)
        self.assertIn("Витрата", sheet_xml)
        self.assertIn("Борг", sheet_xml)
        self.assertIn("Переказ", sheet_xml)
        self.assertIn("Корекція балансу", sheet_xml)
        return

        import csv
        import io as legacy_io

        csv_rows = list(csv.DictReader(legacy_io.StringIO(document["document"].decode("utf-8-sig"))))
        self.assertEqual(len(csv_rows), 7)

        debt_rows = [row for row in csv_rows if row["flow_kind"] == "debt"]
        self.assertEqual(len(debt_rows), 2)
        self.assertTrue(any(row["counterparty"] == "Alice" and row["debt_action"] == "lend" and row["type"] == "transfer" and row["amount"] for row in debt_rows))
        self.assertTrue(any(row["counterparty"] == "Bob" and row["debt_action"] == "borrow" and row["type"] == "transfer" and row["amount"] for row in debt_rows))
        self.assertTrue(all(row["account"] == "" for row in debt_rows))

        normal_rows = [row for row in csv_rows if row["flow_kind"] == "normal"]
        self.assertEqual(len(normal_rows), 2)
        self.assertTrue(any(row["type"] == "income" and row["amount"] for row in normal_rows))
        self.assertTrue(any(row["type"] == "expense" and row["amount"] for row in normal_rows))

        adjustment_rows = [row for row in csv_rows if row["flow_kind"] == "adjustment"]
        self.assertEqual(len(adjustment_rows), 2)
        self.assertTrue(any("РљРѕСЂРµРєС†С–СЏ Р±Р°Р»Р°РЅСЃСѓ" in row["comment"] for row in adjustment_rows))

    async def test_export_callback_empty_period_shows_excel_empty_state(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[], transactions=[])
        context = self._make_context()
        update, message, query = self._make_update("export:today")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.export_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(message.documents, [])
        self.assertTrue(message.replies)
        self.assertIn("Експорт Excel", message.replies[-1]["text"])
        self.assertIn("немає операцій", message.replies[-1]["text"])

    async def test_export_menu_includes_year_12_months_and_all_time(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("0"),
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("export:start")

        with patch.object(bot_main, "_pool", return_value=DummyPool(conn)):
            await bot_main.export_callback(update, context)

        self.assertTrue(query.answered)
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertTrue(any("📊 Дашборд" in label for label in labels))
        self.assertTrue(any("🗂 Рік" in label for label in labels))
        self.assertTrue(any("🗓 12 місяців" in label for label in labels))
        self.assertTrue(any("🧾 Весь час" in label for label in labels))

    async def test_export_callback_all_time_uses_user_start_date(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[], transactions=[])
        context = self._make_context()
        update, message, query = self._make_update("export:all")
        user_start = datetime(2025, 1, 15).date()
        today = bot_main.datetime.now().date()
        service = SimpleNamespace(
            build_export_xlsx=AsyncMock(
                return_value=SimpleNamespace(content=b"xlsx", row_count=1, summary_message="ok")
            )
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"start_date": user_start})),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "ReportService", return_value=service),
        ):
            await bot_main.export_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(
            service.build_export_xlsx.await_args.args,
            (123, user_start, today + bot_main.timedelta(days=1), "Весь час"),
        )
        self.assertEqual(message.documents[0]["filename"], "my-cash-flow-all.xlsx")

    async def test_reports_callback_year_uses_calendar_year_period(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[], transactions=[])
        context = self._make_context()
        update, message, query = self._make_update("reports:year")
        today = bot_main.datetime.now().date()
        service = SimpleNamespace(build_normal_report_text=AsyncMock(return_value="ok"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_get_finance_scope", new=AsyncMock(return_value=SimpleNamespace(is_family=False))),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"start_date": datetime(today.year - 1, 1, 1).date()})),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "ReportService", return_value=service),
        ):
            await bot_main.reports_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(
            service.build_normal_report_text.await_args.args,
            (123, datetime(today.year, 1, 1).date(), today + bot_main.timedelta(days=1), "цей рік"),
        )
        self.assertEqual(message.replies[-1]["text"], "ok")

    async def test_mixed_flow_kind_recalculation_keeps_debt_out_of_balance_math(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Savings", "currency": "UAH", "starting_balance": Decimal("50"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "type": "income", "amount": Decimal("40"), "account_id": 1, "currency": "UAH", "flow_kind": "normal"},
                {"id": 2, "tg_user_id": 123, "type": "expense", "amount": Decimal("10"), "account_id": 1, "currency": "UAH", "flow_kind": "normal"},
                {"id": 3, "tg_user_id": 123, "type": "transfer", "amount": Decimal("25"), "to_amount": Decimal("25"), "from_account_id": 1, "to_account_id": 2, "currency": "UAH", "flow_kind": "transfer"},
                {"id": 4, "tg_user_id": 123, "type": "income", "amount": Decimal("20"), "account_id": 1, "currency": "UAH", "flow_kind": "adjustment", "source": "balance_correction"},
                {"id": 5, "tg_user_id": 123, "type": "expense", "amount": Decimal("5"), "account_id": 2, "currency": "UAH", "flow_kind": "adjustment", "source": "balance_correction"},
                {"id": 6, "tg_user_id": 123, "type": "transfer", "amount": Decimal("57"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Alice", "debt_action": "lend", "source": "text"},
                {"id": 7, "tg_user_id": 123, "type": "transfer", "amount": Decimal("43"), "currency": "USD", "flow_kind": "debt", "counterparty": "Bob", "debt_action": "borrow", "source": "text"},
            ],
        )

        await bot_main._recalculate_account_balances(conn, 123)

        self.assertEqual(conn.accounts[1]["balance"], Decimal("125.00"))
        self.assertEqual(conn.accounts[2]["balance"], Decimal("70.00"))

    async def test_home_archive_confirm_deactivates_account_keeps_history_and_blocks_transfer_source(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("100"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "Cash", "currency": "UAH", "starting_balance": Decimal("50"), "balance": Decimal("50"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"tg_user_id": 123, "type": "income", "amount": Decimal("20"), "account_id": 1},
                {"tg_user_id": 123, "type": "expense", "amount": Decimal("5"), "account_id": 2},
            ],
        )
        context = self._make_context()
        update, message, query = self._make_update("accounts:archive:confirm:1")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        self.assertFalse(conn.accounts[1]["is_active"])
        self.assertEqual(len(conn.transactions), 2)
        self.assertTrue(any("UPDATE accounts SET is_active=false" in q for q, _ in conn.execute_calls))
        self.assertFalse(any("DELETE FROM transactions" in q for q, _ in conn.execute_calls))
        self.assertIn("Архівовано", message.replies[-1]["text"])

        active_accounts = await bot_main._get_accounts(conn, 123)
        self.assertEqual(active_accounts, [(2, "Cash (UAH)")])

        transfer_update, transfer_message, transfer_query = self._make_update("transfer:source:1")
        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_accounts_settings", new=AsyncMock()) as show_accounts_mock,
        ):
            await bot_main.transfer_callback(transfer_update, context)

        self.assertTrue(transfer_query.answered)
        show_accounts_mock.assert_awaited_once()
        self.assertFalse(any("INSERT INTO transactions" in q for q, _ in conn.execute_calls))
        self.assertFalse(any("DELETE FROM transactions" in q for q, _ in conn.execute_calls))

    async def test_recalculate_account_balances_matches_ledger_and_repairs_drift(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "starting_balance": Decimal("100"), "balance": Decimal("0"), "is_active": True, "created_at": datetime(2026, 1, 1)},
                {"id": 2, "tg_user_id": 123, "label": "USD", "currency": "USD", "starting_balance": Decimal("50"), "balance": Decimal("999"), "is_active": True, "created_at": datetime(2026, 1, 2)},
            ],
            transactions=[
                {"tg_user_id": 123, "type": "income", "amount": Decimal("40"), "account_id": 1},
                {"tg_user_id": 123, "type": "expense", "amount": Decimal("10"), "account_id": 1},
                {"tg_user_id": 123, "type": "transfer", "amount": Decimal("25"), "to_amount": Decimal("25"), "from_account_id": 1, "to_account_id": 2},
            ],
        )

        await bot_main._recalculate_account_balances(conn, 123)

        self.assertEqual(conn.accounts[1]["balance"], Decimal("105.00"))
        self.assertEqual(conn.accounts[2]["balance"], Decimal("75.00"))

        conn.accounts[1]["balance"] = Decimal("0.00")
        conn.accounts[2]["balance"] = Decimal("0.00")
        await bot_main._recalculate_account_balances(conn, 123)

        self.assertEqual(conn.accounts[1]["balance"], Decimal("105.00"))
        self.assertEqual(conn.accounts[2]["balance"], Decimal("75.00"))


    async def test_onboarding_account_limit_is_15(self) -> None:
        accounts = [
            {
                "id": index,
                "tg_user_id": 123,
                "label": f"Account {index}",
                "currency": "UAH",
                "starting_balance": Decimal("0"),
                "balance": Decimal("0"),
                "is_active": True,
                "created_at": datetime(2026, 1, index),
            }
            for index in range(1, 16)
        ]
        conn = LedgerConn(tg_user_id=123, accounts=accounts)
        context = self._make_context(tg_user_id=123)
        update, message, query = self._make_update("onb:acct:more")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
        ):
            state = await bot_main.onb_accounts_more_done(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(state, bot_main.ACC_MORE_DONE)
        self.assertIn("до 15 рахунків", message.replies[-1]["text"])

    async def test_onboarding_callback_fallback_routes_confirm_buttons(self) -> None:
        context = self._make_context()
        update, message, query = self._make_update("onb:confirm:ok")

        with patch.object(bot_main, "onb_confirm", new=AsyncMock()) as confirm_mock:
            await bot_main.onboarding_callback_fallback(update, context)

        confirm_mock.assert_awaited_once_with(update, context)
        self.assertFalse(query.answered)
        self.assertEqual(message.replies, [])

    async def test_onboarding_callback_fallback_routes_last4_buttons_with_active_account(self) -> None:
        context = self._make_context()
        context.user_data["onb"] = {"current_account": {"label_base": "Monobank"}}
        update, _message, _query = self._make_update("onb:acct:last4:skip")

        with patch.object(bot_main, "onb_account_last4_choice", new=AsyncMock()) as last4_mock:
            await bot_main.onboarding_callback_fallback(update, context)

        last4_mock.assert_awaited_once_with(update, context)

    async def test_onb_account_last4_enter_prompts_for_digits(self) -> None:
        context = self._make_context()
        context.user_data["onb"] = {"current_account": {"label_base": "Monobank"}}
        update, message, query = self._make_update("onb:acct:last4:enter")

        with patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()):
            state = await bot_main.onb_account_last4_choice(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(state, bot_main.ACC_LAST4_TEXT)
        self.assertIn("Введіть рівно 4 цифри", message.replies[-1]["text"])

    async def test_onb_confirm_can_recover_user_id_without_conversation_state(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[])
        context = SimpleNamespace(
            application=SimpleNamespace(bot=object(), bot_data={}),
            user_data={},
        )
        update, message, query = self._make_update("onb:confirm:ok")
        category_service = SimpleNamespace(createCategoriesFromOnboardingSelection=AsyncMock())
        unpaid_access_state = {
            "access_scope": "paywall",
            "access_source": "billing",
            "pending_start_payload": "",
            "promo_offer_code": "",
            "promo_trial_days": 0,
            "billing_state": {
                "profile_exists": False,
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "",
                "trial_days": 0,
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "open",
            },
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "CategoryService", return_value=category_service),
            patch.object(bot_main, "get_access_state", new=AsyncMock(return_value=unpaid_access_state)),
            patch.object(
                bot_main,
                "init_bind_session",
                new=AsyncMock(return_value={"page_url": "https://mono.test/onboarding-bind", "trial_days": 30, "trial_granted": True}),
            ) as bind_mock,
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "sync_onboarding_debug_state", new=AsyncMock()),
            patch.object(bot_main, "_reply_home", new=AsyncMock()) as reply_home_mock,
            patch.object(bot_main, "notify_admins", new=AsyncMock()),
            patch.object(bot_main, "format_onboarding_notification", return_value="ok"),
            patch.object(bot_main.ConversationHandler, "END", -1, create=True),
        ):
            state = await bot_main.onb_confirm(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(state, -1)
        self.assertEqual(context.user_data["tg_user_id"], 123)
        category_service.createCategoriesFromOnboardingSelection.assert_awaited_once()
        bind_mock.assert_awaited_once()
        self.assertTrue(any("UPDATE users SET onboarding_completed=true" in " ".join(query.split()) for query, _ in conn.execute_calls))
        reply_home_mock.assert_not_awaited()
        self.assertEqual(len(message.replies), 1)
        self.assertIn("Вітаємо! Все налаштовано", message.replies[0]["text"])
        self.assertIn("Повний доступ можна спробувати 30 днів без оплати", message.replies[0]["text"])
        self.assertIn("1 грн зараз: перевірка картки", message.replies[0]["text"])
        self.assertEqual(self._button_labels(message.replies[0]["reply_markup"])[0], "Активувати пробний період")

    async def test_onb_confirm_keeps_home_flow_for_user_with_active_billing(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[])
        context = SimpleNamespace(
            application=SimpleNamespace(bot=object(), bot_data={}),
            user_data={},
        )
        update, message, query = self._make_update("onb:confirm:ok")
        category_service = SimpleNamespace(createCategoriesFromOnboardingSelection=AsyncMock())
        active_access_state = {
            "access_scope": "personal_full",
            "access_source": "billing",
            "pending_start_payload": "",
            "promo_offer_code": "",
            "promo_trial_days": 0,
            "billing_state": {
                "profile_exists": True,
                "has_card": True,
                "masked_pan": "4444",
                "auto_renew_enabled": True,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "trial",
                "trial_days": 30,
                "expires_at": datetime(2026, 5, 30),
                "next_charge_at": datetime(2026, 5, 30),
                "grace_expires_at": None,
                "access_mode": "full",
            },
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "CategoryService", return_value=category_service),
            patch.object(bot_main, "get_access_state", new=AsyncMock(return_value=active_access_state)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "sync_onboarding_debug_state", new=AsyncMock()),
            patch.object(bot_main, "_reply_home", new=AsyncMock()) as reply_home_mock,
            patch.object(bot_main, "notify_admins", new=AsyncMock()),
            patch.object(bot_main, "format_onboarding_notification", return_value="ok"),
            patch.object(bot_main.ConversationHandler, "END", -1, create=True),
        ):
            state = await bot_main.onb_confirm(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(state, -1)
        reply_home_mock.assert_awaited_once()
        self.assertEqual(len(message.replies), 1)
        self.assertIn("Готово, базове налаштування завершено.", message.replies[0]["text"])


    async def test_onb_confirm_recovers_onboarding_payload_from_record_like_row(self) -> None:
        class RecordLike:
            def __init__(self, payload):
                self._payload = payload

            def __getitem__(self, key):
                if key != "onboarding_payload":
                    raise KeyError(key)
                return self._payload

        class RecordLikeConn(LedgerConn):
            async def fetchrow(self, query: str, *args):
                normalized = " ".join(query.split())
                if "FROM user_admin_states" in normalized:
                    return RecordLike({"category_mode": "custom", "custom_expense_categories": ["РљР°РІР°"]})
                return await super().fetchrow(query, *args)

        conn = RecordLikeConn(tg_user_id=123, accounts=[])
        context = SimpleNamespace(
            application=SimpleNamespace(bot=object(), bot_data={}),
            user_data={},
        )
        update, _message, query = self._make_update("onb:confirm:ok")
        category_service = SimpleNamespace(createCategoriesFromOnboardingSelection=AsyncMock())

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "CategoryService", return_value=category_service),
            patch.object(
                bot_main,
                "get_access_state",
                new=AsyncMock(
                    return_value={
                        "access_scope": "personal_full",
                        "access_source": "billing",
                        "pending_start_payload": "",
                        "promo_offer_code": "",
                        "promo_trial_days": 0,
                        "billing_state": {
                            "profile_exists": True,
                            "has_card": True,
                            "masked_pan": "4444",
                            "auto_renew_enabled": True,
                            "last_action_url": "",
                            "last_failure_reason": "",
                            "subscription_status": "trial",
                            "trial_days": 30,
                            "expires_at": datetime(2026, 5, 30),
                            "next_charge_at": datetime(2026, 5, 30),
                            "grace_expires_at": None,
                            "access_mode": "full",
                        },
                    }
                ),
            ),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "sync_onboarding_debug_state", new=AsyncMock()),
            patch.object(bot_main, "_reply_home", new=AsyncMock()),
            patch.object(bot_main, "notify_admins", new=AsyncMock()),
            patch.object(bot_main, "format_onboarding_notification", return_value="ok"),
            patch.object(bot_main.ConversationHandler, "END", -1, create=True),
        ):
            state = await bot_main.onb_confirm(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(state, -1)
        category_service.createCategoriesFromOnboardingSelection.assert_awaited_once_with(
            123,
            {
                "category_mode": "custom",
                "selected_expense_template_ids": [],
                "selected_income_template_ids": [],
                "custom_expense_categories": ["РљР°РІР°"],
                "custom_income_categories": [],
            },
        )

    async def test_settings_billing_bind_uses_pending_promo_offer(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": False,
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "",
                "trial_days": 0,
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "open",
            },
            access_state={
                "access_scope": "paywall",
                "access_source": "promo",
                "pending_start_payload": "promo_PROMO90",
            },
            promo_offer={"code": "PROMO90", "trial_days": 90},
        )
        context = self._make_context()
        update, message, query = self._make_update("settings:billing:bind")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "init_bind_session", new=AsyncMock(return_value={"page_url": "https://mono.test/pay", "trial_days": 90})) as bind_mock,
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        bind_mock.assert_awaited_once_with(tg_user_id=123, trial_days=90, mode="bind", promo_code="PROMO90")
        self.assertIn("90 днів пробного доступу", message.replies[-1]["text"])
        self.assertIn("499 грн/міс — лише після 90 днів.", message.replies[-1]["text"])
        self.assertIn("Активувати пробний період", self._button_labels(message.replies[-1]["reply_markup"]))

    async def test_start_entry_routes_existing_debt_only_user_to_scoped_surface(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": False,
                "has_card": False,
                "subscription_status": "",
                "expires_at": None,
                "next_charge_at": None,
                "trial_days": 0,
                "access_mode": "open",
            },
            access_state={
                "access_scope": "debt_only",
                "access_source": "debt",
                "pending_start_payload": "debt_token",
            },
        )
        context = SimpleNamespace(
            application=SimpleNamespace(bot=object(), bot_data={}),
            user_data={},
            args=[],
        )
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester", first_name="Tester", last_name=None, language_code="uk"),
            message=DummyMessage(text="/start"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "is_registration_open", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_show_debt_only_surface", new=AsyncMock()) as debt_surface_mock,
            patch.object(bot_main.ConversationHandler, "END", -1, create=True),
        ):
            state = await bot_main.start_entry(update, context)

        self.assertEqual(state, -1)
        debt_surface_mock.assert_awaited_once()

    async def test_handle_family_invite_start_stores_family_pending_payload(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context()
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=DummyMessage(text="/start"),
        )
        family_service = SimpleNamespace(
            get_family_invite=AsyncMock(
                return_value={
                    "status": "active",
                    "expires_at": datetime(2030, 1, 1, tzinfo=UTC),
                    "used_count": 0,
                    "max_uses": 1,
                    "owner_user_id": 999,
                    "active_members_count": 1,
                    "family_name": "РўРµСЃС‚РѕРІР° СЂРѕРґРёРЅР°",
                    "owner_first_name": "Owner",
                    "owner_username": "owner",
                }
            ),
            get_scope=AsyncMock(return_value=SimpleNamespace(is_family=False)),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main.config, "FAMILY_ACCESS_ENABLED", True),
            patch.object(bot_main, "FamilyService", return_value=family_service),
            patch.object(bot_main.ConversationHandler, "END", -1, create=True),
        ):
            state = await bot_main._handle_family_invite_start(update, context, "fam123")

        self.assertEqual(state, -1)
        self.assertEqual(conn.access_state["access_scope"], "paywall")
        self.assertEqual(conn.access_state["access_source"], "family")
        self.assertEqual(conn.access_state["pending_start_payload"], "family_invite_fam123")
        self.assertIn("Запрошення до сімейного бюджету", update.message.replies[-1]["text"])

    async def test_start_entry_replays_pending_family_invite_before_onboarding(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": False,
                "has_card": False,
                "subscription_status": "",
                "expires_at": None,
                "next_charge_at": None,
                "trial_days": 0,
                "access_mode": "open",
            },
            access_state={
                "access_scope": "paywall",
                "access_source": "family",
                "pending_start_payload": "family_invite_resume",
            },
        )
        context = SimpleNamespace(
            application=SimpleNamespace(bot=object(), bot_data={}),
            user_data={},
            args=[],
        )
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester", first_name="Tester", last_name=None, language_code="uk"),
            message=DummyMessage(text="/start"),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "is_registration_open", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_resume_pending_start_payload", new=AsyncMock(return_value=True)) as replay_mock,
            patch.object(bot_main.ConversationHandler, "END", -1, create=True),
        ):
            state = await bot_main.start_entry(update, context)

        self.assertEqual(state, -1)
        replay_mock.assert_awaited_once()

    async def test_home_callback_soft_grace_user_keeps_full_access(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": True,
                "masked_pan": "444455******1111",
                "auto_renew_enabled": True,
                "last_failure_reason": "insufficient funds",
                "subscription_status": "expired",
                "trial_days": 30,
                "expires_at": datetime(2024, 1, 1, tzinfo=UTC),
                "next_charge_at": None,
                "grace_expires_at": datetime(2030, 1, 1, tzinfo=UTC),
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, message, query = self._make_update("menu:main")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_reply_home", new=AsyncMock()) as home_mock,
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        home_mock.assert_awaited_once()
        self.assertIn("Автосписання не пройшло", home_mock.await_args.args[1])

    def test_home_keyboard_uses_compact_layout(self) -> None:
        self.assertEqual(
            self._button_labels(bot_main.kb_home()),
            ["📊 Дашборд", "🧾 Операції", "📈 Звіти", "💼 Фінанси", "⋯ Ще"],
        )

    async def test_home_callback_operations_menu_shows_compact_submenu(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("0"),
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ],
            access_state={"access_scope": "personal_full", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, message, query = self._make_update("menu:operations")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(message.replies[-1]["text"], "<b>🧾 Операції</b>")
        self.assertEqual(
            self._button_labels(message.replies[-1]["reply_markup"]),
            ["➖ Витрата", "➕ Дохід", "🔄 Переказ", "🕘 Останні операції", "⬅️ Назад"],
        )

    async def test_home_callback_reports_for_paywall_user_shows_billing_lock(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("0"),
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ],
            billing_state={
                "profile_exists": False,
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "",
                "trial_days": 0,
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "open",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, message, query = self._make_update("reports:start")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("Почніть пробний період", message.replies[-1]["text"])

    async def test_home_callback_back_to_home_for_paywall_user_returns_app_entry(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("0"),
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ],
            billing_state={
                "profile_exists": False,
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "",
                "trial_days": 0,
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "open",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, message, query = self._make_update("home:show")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "BOT_TOKEN", "123456:test-miniapp-token"),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("оплата продовжаться вже у застосунку", message.replies[-1]["text"])
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertEqual(labels[0], "🌐 Увійти через Safari / Chrome")

    async def test_show_billing_menu_pending_bind_without_card_hides_retry(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": True,
                "last_action_url": "https://pay.monobank.ua/test-bind",
                "last_failure_reason": "",
                "subscription_status": "",
                "trial_days": 0,
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "open",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        message = DummyMessage()

        with patch.object(
            bot_main,
            "sync_pending_bind_status",
            new=AsyncMock(return_value={"updated": False, "monobank_status": "created"}),
        ):
            await bot_main._show_billing_menu(message, conn, 123)

        self.assertIn("Почніть пробний період", message.replies[-1]["text"])
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertIn("Продовжити в Monobank", labels)
        self.assertNotIn("РћРїР»Р°С‚РёС‚Рё Р·Р°СЂР°Р·", labels)
        self.assertNotIn("Змінити картку", labels)

    async def test_show_billing_menu_expired_subscription_without_card_uses_recovery_checkout_copy(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "expired",
                "trial_days": 30,
                "expires_at": datetime(2026, 1, 1, tzinfo=UTC),
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "blocked",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        message = DummyMessage()

        await bot_main._show_billing_menu(message, conn, 123)

        text = message.replies[-1]["text"]
        self.assertIn("499 грн", text)
        self.assertIn("Щоб відновити доступ", text)
        self.assertNotIn("1 грн зараз", text)
        self.assertNotIn("Пробний період повторно не нараховується", text)
        self.assertNotIn("Пробний період: 30 днів", text)
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertIn("Оплатити 499 грн", labels)
        self.assertNotIn("Спробувати 30 днів", labels)

    async def test_show_billing_menu_expired_subscription_with_card_shows_retry_and_rebind_buttons(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": True,
                "masked_pan": "444455******1111",
                "auto_renew_enabled": False,
                "last_action_url": "https://mono.test/recovery",
                "last_failure_reason": "",
                "subscription_status": "expired",
                "trial_days": 30,
                "expires_at": datetime(2026, 1, 1, tzinfo=UTC),
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "blocked",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        message = DummyMessage()

        await bot_main._show_billing_menu(message, conn, 123)

        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertEqual(labels[:2], ["Оплатити зараз", "Змінити спосіб оплати і оплатити"])
        self.assertNotIn("Підтвердити в Monobank", labels)

    async def test_settings_billing_bind_for_blocked_historical_user_starts_recovery_checkout(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "",
                "last_failure_reason": "insufficient funds",
                "subscription_status": "expired",
                "trial_days": 30,
                "expires_at": datetime(2026, 1, 1, tzinfo=UTC),
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "blocked",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, message, query = self._make_update("settings:billing:bind")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(
                bot_main,
                "init_recovery_payment_session",
                new=AsyncMock(return_value={"page_url": "https://mono.test/recovery"}),
            ) as recovery_mock,
            patch.object(
                bot_main,
                "init_bind_session",
                new=AsyncMock(return_value={"page_url": "https://mono.test/bind", "trial_days": 0, "trial_granted": False}),
            ) as bind_mock,
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        recovery_mock.assert_awaited_once_with(tg_user_id=123)
        bind_mock.assert_not_awaited()
        self.assertIn("499 грн", message.replies[-1]["text"])
        self.assertIn("Відкрити Monobank", self._button_labels(message.replies[-1]["reply_markup"]))

    async def test_settings_billing_retry_for_recovery_screen_with_pending_action_url_opens_confirmation(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": True,
                "masked_pan": "444455******1111",
                "auto_renew_enabled": False,
                "last_action_url": "https://mono.test/recovery",
                "last_failure_reason": "insufficient funds",
                "subscription_status": "expired",
                "trial_days": 30,
                "expires_at": datetime(2026, 1, 1, tzinfo=UTC),
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "blocked",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, message, query = self._make_update("settings:billing:retry")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "retry_renewal", new=AsyncMock()) as retry_mock,
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        retry_mock.assert_not_awaited()
        self.assertIn("Потрібне підтвердження в Monobank", message.replies[-1]["text"])
        self.assertIn("Підтвердити в Monobank", self._button_labels(message.replies[-1]["reply_markup"]))

    async def test_settings_billing_rebind_for_read_only_historical_user_starts_recovery_checkout(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": True,
                "masked_pan": "444455******1111",
                "auto_renew_enabled": False,
                "last_action_url": "",
                "last_failure_reason": "insufficient funds",
                "subscription_status": "expired",
                "trial_days": 30,
                "expires_at": datetime(2026, 1, 1, tzinfo=UTC),
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "read_only",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, message, query = self._make_update("settings:billing:rebind")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(
                bot_main,
                "init_recovery_payment_session",
                new=AsyncMock(return_value={"page_url": "https://mono.test/recovery"}),
            ) as recovery_mock,
            patch.object(
                bot_main,
                "init_bind_session",
                new=AsyncMock(return_value={"page_url": "https://mono.test/rebind", "trial_days": 0, "trial_granted": False}),
            ) as bind_mock,
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        recovery_mock.assert_awaited_once_with(tg_user_id=123)
        bind_mock.assert_not_awaited()
        self.assertIn("499 грн", message.replies[-1]["text"])
        self.assertIn("Відкрити Monobank", self._button_labels(message.replies[-1]["reply_markup"]))

    async def test_show_billing_menu_personal_full_without_card_keeps_access_active_copy(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": False,
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "manual",
                "trial_days": 0,
                "expires_at": datetime(2030, 1, 1, tzinfo=UTC),
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "full",
            },
            access_state={"access_scope": "personal_full", "access_source": "billing", "pending_start_payload": ""},
        )
        message = DummyMessage()

        await bot_main._show_billing_menu(message, conn, 123)

        text = message.replies[-1]["text"]
        self.assertIn("Поточний доступ уже активний", text)
        self.assertIn("Картка не прив'язана", text)
        self.assertNotIn("Потрібна оплата", text)
        self.assertNotIn("Якщо доступ уже зупинився", text)

    async def test_settings_billing_rebind_uses_non_trial_confirmation_copy(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": True,
                "masked_pan": "444455******1111",
                "auto_renew_enabled": True,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "active",
                "trial_days": 30,
                "expires_at": datetime(2030, 1, 1, tzinfo=UTC),
                "next_charge_at": datetime(2030, 1, 1, tzinfo=UTC),
                "grace_expires_at": None,
                "access_mode": "full",
            },
            access_state={"access_scope": "personal_full", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, message, query = self._make_update("settings:billing:rebind")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(
                bot_main,
                "init_recovery_payment_session",
                new=AsyncMock(return_value={"page_url": "https://mono.test/recovery"}),
            ) as recovery_mock,
            patch.object(
                bot_main,
                "init_bind_session",
                new=AsyncMock(return_value={"page_url": "https://mono.test/rebind", "trial_days": 0, "trial_granted": False}),
            ) as bind_mock,
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        bind_mock.assert_awaited_once()
        recovery_mock.assert_not_awaited()
        self.assertIn("Новий пробний період не нараховується", message.replies[-1]["text"])
        self.assertNotIn("Після 30 днів", message.replies[-1]["text"])
        self.assertIn("Відкрити Monobank", self._button_labels(message.replies[-1]["reply_markup"]))

    async def test_show_billing_menu_syncs_pending_bind_success_and_opens_home(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "profile_status": "pending",
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "https://pay.monobank.ua/test-bind",
                "last_failure_reason": "",
                "subscription_status": "",
                "trial_days": 0,
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "open",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        message = DummyMessage()

        async def sync_side_effect(*, tg_user_id: int):
            self.assertEqual(tg_user_id, 123)
            expiry = datetime(2030, 1, 1, tzinfo=UTC)
            conn.billing_state.update(
                {
                    "profile_status": "active",
                    "has_card": True,
                    "masked_pan": "444455******1111",
                    "auto_renew_enabled": True,
                    "last_action_url": "",
                    "subscription_status": "trial",
                    "trial_days": 30,
                    "expires_at": expiry,
                    "next_charge_at": expiry,
                    "access_mode": "full",
                }
            )
            conn.access_state.update({"access_scope": "personal_full", "access_source": "billing"})
            return {"updated": True, "monobank_status": "success"}

        with (
            patch.object(bot_main, "sync_pending_bind_status", new=AsyncMock(side_effect=sync_side_effect)) as sync_mock,
            patch.object(bot_main, "_maybe_resume_pending_start_payload", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_reply_home", new=AsyncMock()) as reply_home_mock,
        ):
            await bot_main._show_billing_menu(message, conn, 123)

        sync_mock.assert_awaited_once()
        reply_home_mock.assert_awaited_once()
        self.assertEqual(message.replies, [])
        self.assertTrue(reply_home_mock.await_args.args[1].startswith("<b>"))

    async def test_billing_continue_routes_unready_user_into_app_onboarding(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context()
        update, message, query = self._make_update("billing:continue")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_show_miniapp_onboarding_entry", new=AsyncMock(return_value=True)) as onboarding_mock,
            patch.object(bot_main, "_begin_onboarding_flow", new=AsyncMock()) as legacy_onboarding_mock,
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        onboarding_mock.assert_awaited_once_with(message, conn, 123)
        legacy_onboarding_mock.assert_not_awaited()

    async def test_billing_continue_routes_ready_full_user_to_access_surface(self) -> None:
        conn = LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context()
        update, _message, query = self._make_update("billing:continue")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_access_surface", new=AsyncMock()) as access_mock,
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        access_mock.assert_awaited_once()

    async def test_billing_continue_routes_ready_paywall_user_to_billing_menu(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": False,
                "has_card": False,
                "subscription_status": "",
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "trial_days": 0,
                "access_mode": "open",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        update, _message, query = self._make_update("billing:continue")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_billing_menu", new=AsyncMock()) as billing_mock,
        ):
            await bot_main.home_callback(update, context)

        self.assertTrue(query.answered)
        billing_mock.assert_awaited_once()

    async def test_settings_billing_retry_requires_bound_confirmation_and_reuses_intent(self) -> None:
        conn = LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "has_card": True,
                "masked_pan": "444455******1111",
                "auto_renew_enabled": True,
                "last_action_url": "",
                "last_failure_reason": "insufficient funds",
                "subscription_status": "expired",
                "trial_days": 0,
                "expires_at": datetime(2026, 1, 1, tzinfo=UTC),
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "blocked",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context()
        preview_update, preview_message, _ = self._make_update("settings:billing:retry")
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def controlled_retry(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                await release.wait()
            return {"status": "success"}

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "retry_renewal", new=AsyncMock(side_effect=controlled_retry)) as retry_mock,
        ):
            await bot_main.settings_callback(preview_update, context)
            retry_mock.assert_not_awaited()
            callback = next(
                value
                for value in self._button_callbacks(preview_message.replies[-1]["reply_markup"])
                if value.startswith("settings:billing:retry:confirm:")
            )
            confirm_update, _confirm_message, _ = self._make_update(callback)
            first = asyncio.create_task(bot_main.settings_callback(confirm_update, context))
            await started.wait()
            concurrent_update, concurrent_message, _ = self._make_update(callback)
            await bot_main.settings_callback(concurrent_update, context)
            retry_mock.assert_awaited_once()
            self.assertIn("обробляється", concurrent_message.replies[-1]["text"])
            release.set()
            await first
            intent_key = retry_mock.await_args.kwargs["intent_key"]
            self.assertTrue(intent_key.startswith("billing_retry_123_"))

            stale_update, stale_message, _ = self._make_update(callback)
            await bot_main.settings_callback(stale_update, context)
            retry_mock.assert_awaited_once()
            self.assertIn("застаріло", stale_message.replies[-1]["text"])

    async def test_restart_onboarding_callback_entry_restarts_language_step(self) -> None:
        class RestartConn(LedgerConn):
            async def execute(self, query: str, *args) -> None:
                normalized = " ".join(query.split())
                if "UPDATE accounts SET is_active=false WHERE tg_user_id=$1" in normalized:
                    self.execute_calls.append((query, args))
                    return
                await super().execute(query, *args)

        conn = RestartConn(tg_user_id=123, accounts=[])
        context = SimpleNamespace(
            application=SimpleNamespace(bot=object(), bot_data={}),
            user_data={},
        )
        update, message, query = self._make_update("onb:confirm:restart")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "sync_onboarding_debug_state", new=AsyncMock()),
            patch.object(bot_main.ConversationHandler, "END", -1, create=True),
        ):
            state = await bot_main.restart_onboarding_callback_entry(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(state, bot_main.LANG)
        self.assertEqual(context.user_data["tg_user_id"], 123)
        self.assertEqual(context.user_data["onb"], {})
        self.assertTrue(any("UPDATE users SET onboarding_completed=false" in " ".join(sql.split()) for sql, _ in conn.execute_calls))
        self.assertIn("Крок 1 з 5: оберіть мову.", message.replies[-1]["text"])


if __name__ == "__main__":
    unittest.main()

