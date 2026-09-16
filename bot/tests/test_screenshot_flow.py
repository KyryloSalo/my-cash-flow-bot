from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import UTC, date, datetime
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

    class _Filter:
        def __and__(self, other):
            return self

        def __or__(self, other):
            return self

        def __invert__(self):
            return self

    ext_stub.Application = _Stub
    ext_stub.CallbackQueryHandler = _Stub
    ext_stub.CommandHandler = _Stub
    ext_stub.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
    ext_stub.Defaults = _Stub
    ext_stub.ConversationHandler = _Stub
    ext_stub.MessageHandler = _Stub
    ext_stub.filters = types.SimpleNamespace(
        TEXT=_Filter(),
        COMMAND=_Filter(),
        VOICE=_Filter(),
        PHOTO=_Filter(),
        Document=types.SimpleNamespace(IMAGE=_Filter()),
    )
    sys.modules["telegram.ext"] = ext_stub

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")

    class _AsyncClient:
        pass

    httpx_stub.AsyncClient = _AsyncClient
    httpx_stub.HTTPError = RuntimeError
    sys.modules["httpx"] = httpx_stub

import bot_main  # noqa: E402
from tests.financial_fixtures import confirmation_from_reply, prepare_confirmation
from ai_transaction_draft_service import AiTransactionDraft, AiTransactionDraftService  # noqa: E402
from vision_tx import OpenAIVisionError, OpenAIVisionInvalidResponseError, ScreenshotAnalysis, StatementItemAnalysis  # noqa: E402


class DummyFile:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    async def download_as_bytearray(self):
        return bytearray(self.payload)


class DummyPhoto:
    def __init__(self, payload: bytes, *, file_id: str = "photo-file", file_unique_id: str = "photo-unique", file_size: int = 128) -> None:
        self.file_id = file_id
        self.file_unique_id = file_unique_id
        self.file_size = file_size
        self._file = DummyFile(payload)

    async def get_file(self):
        return self._file


class DummyDocument:
    def __init__(
        self,
        payload: bytes,
        *,
        file_id: str = "doc-file",
        file_unique_id: str = "doc-unique",
        file_size: int = 128,
        mime_type: str = "image/png",
    ) -> None:
        self.file_id = file_id
        self.file_unique_id = file_unique_id
        self.file_size = file_size
        self.mime_type = mime_type
        self._file = DummyFile(payload)

    async def get_file(self):
        return self._file


class DummyMessage:
    def __init__(self, *, text: str = "", photo: list | None = None, document=None) -> None:
        self.text = text
        self.photo = photo or []
        self.document = document
        self.message_id = 77
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
        accounts_by_id: dict[int, dict] | None = None,
        recent_account_id_by_currency: dict[str, int] | None = None,
        recent_categorized_transactions: list[dict] | None = None,
    ) -> None:
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.drafts: dict[int, dict] = {}
        self.draft_writes: list[tuple[str, tuple]] = []
        self.accounts_by_id = accounts_by_id or {}
        self.recent_account_id_by_currency = {str(key): int(value) for key, value in (recent_account_id_by_currency or {}).items()}
        self.recent_categorized_transactions = list(recent_categorized_transactions or [])
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

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        normalized = " ".join(query.split())
        if "SELECT category_id, account_id, comment FROM transactions" in normalized:
            return [dict(row) for row in self.recent_categorized_transactions]
        return []

    def seed_draft(self, flow: dict) -> None:
        """Prepare the persisted row corresponding to this explicit test draft."""
        tx = flow.get("tx") or (flow.get("items") or [{}])[0]
        draft_id = int(flow["draft_id"])
        self.drafts[draft_id] = {
            "id": draft_id, "tg_user_id": 123, "status": "pending",
            "source": flow.get("draft_source", "screenshot"),
            "telegram_file_unique_id": f"fixture-{draft_id}",
            "transaction_date": date.fromisoformat(tx["date"]),
            "tx_type": tx.get("type"), "amount": tx.get("amount"),
            "currency": tx.get("currency"),
            "account_id": tx.get("account_id", flow.get("account_id")),
            "category_id": tx.get("category_id"), "comment": tx.get("comment"),
            "metadata": flow.get("draft_metadata", {}),
        }

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "ai_transaction_drafts" in normalized:
            row = self.drafts.get(int(args[0]))
            if row is None or row["tg_user_id"] != int(args[1]):
                return None
            if normalized.startswith("UPDATE ai_transaction_drafts"):
                if row["status"] != "pending":
                    return None
                self.draft_writes.append((query, args))
                if "SET status='completed'" in normalized:
                    row["status"] = "completed"
                    row["confirmed_at"] = datetime.now(UTC)
                elif "SET transaction_date=$3" in normalized:
                    names = ("transaction_date", "tx_type", "amount", "currency", "account_id", "category_id", "comment", "confidence")
                    row.update(zip(names, args[2:10]))
                    if args[10] is not None:
                        import json
                        row["metadata"] = json.loads(args[10])
            return dict(row)
        if "FROM family_members fm" in normalized:
            return None
        if "SELECT t.account_id FROM transactions t JOIN accounts a" in normalized:
            currency = str(args[-1])
            account_id = self.recent_account_id_by_currency.get(currency)
            return {"account_id": account_id} if account_id is not None else None
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


class ScreenshotFlowTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _button_labels(reply_markup) -> list[str]:
        labels: list[str] = []
        rows = reply_markup.args[0] if reply_markup and getattr(reply_markup, "args", None) else []
        for row in rows:
            for button in row:
                labels.append(button.args[0] if getattr(button, "args", None) else "")
        return labels

    def _make_context(self):
        return SimpleNamespace(application=SimpleNamespace(bot=SimpleNamespace()), user_data={})

    def _make_photo_update(self, payload: bytes = b"img-bytes"):
        message = DummyMessage(photo=[DummyPhoto(payload)])
        user = SimpleNamespace(id=123, username="tester")
        return SimpleNamespace(effective_user=user, message=message), message

    def _make_callback_update(self, message: DummyMessage, callback_data: str):
        user = SimpleNamespace(id=123, username="tester")
        query = DummyQuery(callback_data, message)
        return SimpleNamespace(effective_user=user, message=message, callback_query=query), query

    def _make_text_update(self, text: str):
        message = DummyMessage(text=text)
        user = SimpleNamespace(id=123, username="tester")
        return SimpleNamespace(effective_user=user, message=message), message

    def _catalog(self) -> dict:
        return {
            "accounts_rows": [(11, "Mono 1111")],
            "accounts_full": [{"id": 11, "label": "Mono 1111", "currency": "UAH", "balance": Decimal("0")}],
            "expense_full": [(31, "Продукти", ["groceries", "магазин"])],
            "income_full": [(41, "Зарплата", ["зарплата"])],
            "expense_rows": [(31, "Продукти")],
            "income_rows": [(41, "Зарплата")],
            "accounts_map": {11: "Mono 1111"},
            "categories_map": {31: "Продукти", 41: "Зарплата"},
        }

    def _draft(self, *, status: str = "pending", metadata: dict | None = None) -> AiTransactionDraft:
        return AiTransactionDraft(
            id=5,
            tg_user_id=123,
            source=bot_main.AI_SCREENSHOT_SOURCE,
            telegram_file_unique_id="photo-unique",
            telegram_file_id="photo-file",
            telegram_message_id=77,
            status=status,
            transaction_date=date(2026, 5, 3),
            tx_type="income",
            amount=Decimal("2500.00"),
            currency="UAH",
            account_id=11,
            category_id=41,
            comment="Клієнт оплатив",
            confidence=0.91,
            metadata=metadata
            or {
                "model": "gpt-5.4-nano",
                "screenshot_draft_schema_version": bot_main.AI_SCREENSHOT_DRAFT_SCHEMA_VERSION,
            },
            created_at=None,
            updated_at=None,
            confirmed_at=None,
            cancelled_at=None,
        )

    def _billing_draft(
        self,
        *,
        draft_id: int = 5,
        status: str = "pending",
        account_id: int | None = 11,
        category_id: int | None = 51,
        metadata: dict | None = None,
    ) -> AiTransactionDraft:
        return AiTransactionDraft(
            id=draft_id,
            tg_user_id=123,
            source=bot_main.BILLING_TX_DRAFT_SOURCE,
            telegram_file_unique_id=f"billing-{draft_id}",
            telegram_file_id=None,
            telegram_message_id=None,
            status=status,
            transaction_date=date(2026, 5, 25),
            tx_type="expense",
            amount=Decimal("499.00"),
            currency="UAH",
            account_id=account_id,
            category_id=category_id,
            comment="Оплата підписки vydno.capital",
            confidence=None,
            metadata=metadata
            or {
                "origin": bot_main.BILLING_TX_DRAFT_SOURCE,
                "source": f"billing_paid_{draft_id}",
                "category_slug": "subscriptions_services",
            },
            created_at=None,
            updated_at=None,
            confirmed_at=None,
            cancelled_at=None,
        )

    def _analysis(self) -> ScreenshotAnalysis:
        return ScreenshotAnalysis(
            transaction_date=date(2026, 5, 3),
            type="income",
            amount=Decimal("2500.00"),
            currency="UAH",
            account_hint="mono",
            category_hint="зарплата",
            comment="Клієнт оплатив",
            confidence=0.91,
            bank_name="monobank",
            card_last4="1111",
            model="gpt-5.4-nano",
            detail="low",
            used_fallback=False,
            raw_response=None,
        )

    def _statement_analysis(self) -> ScreenshotAnalysis:
        return ScreenshotAnalysis(
            transaction_date=date(2026, 5, 3),
            type="expense",
            amount=None,
            currency="UAH",
            account_hint="mono",
            category_hint="магазин",
            comment="Виписка",
            confidence=0.93,
            bank_name="monobank",
            card_last4="1111",
            model="gpt-5.4-nano",
            detail="high",
            used_fallback=True,
            raw_response=None,
            mode="statement_expenses",
            dominant_currency="UAH",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 5, 3),
                    type="expense",
                    amount=Decimal("120.00"),
                    currency="UAH",
                    category_hint="магазин",
                    comment="АТБ",
                    confidence=0.94,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 5, 4),
                    type="expense",
                    amount=Decimal("80.00"),
                    currency="UAH",
                    category_hint="магазин",
                    comment="Сільпо",
                    confidence=0.91,
                ),
            ),
            skipped_items_count=1,
        )

    def _statement_draft(self, *, status: str = "pending") -> AiTransactionDraft:
        return self._draft(
            status=status,
            metadata={
                "mode": "statement_expenses",
                "model": "gpt-5.4-nano",
                "screenshot_draft_schema_version": bot_main.AI_SCREENSHOT_DRAFT_SCHEMA_VERSION,
                "dominant_currency": "UAH",
                "statement_items": [
                    {
                        "date": "2026-05-03",
                        "type": "expense",
                        "amount": "120.00",
                        "currency": "UAH",
                        "category_id": 31,
                        "comment": "АТБ",
                        "source": bot_main.AI_SCREENSHOT_SOURCE,
                    },
                    {
                        "date": "2026-05-04",
                        "type": "expense",
                        "amount": "80.00",
                        "currency": "UAH",
                        "category_id": 31,
                        "comment": "Сільпо",
                        "source": bot_main.AI_SCREENSHOT_SOURCE,
                    },
                ],
                "statement_skipped_items_count": 1,
            },
        )

    async def test_screenshot_happy_path_creates_preview_and_confirms(self) -> None:
        account = {
            "id": 11,
            "tg_user_id": 123,
            "label": "Mono 1111",
            "currency": "UAH",
            "balance": Decimal("100.00"),
            "account_type": "main",
            "credit_limit": None,
            "monthly_interest_rate": None,
            "non_negative_account_type": "main",
        }
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._draft(), True)),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=self._analysis())),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        self.assertIn("Чернетку зі скріншоту підготовлено", message.replies[0]["text"])
        self.assertIn("Перевірте чернетку", message.replies[1]["text"])
        self.assertEqual(context.user_data["ai_tx_flow"]["draft_id"], 5)
        self.assertEqual(conn.execute_calls, [])
        conn.seed_draft(context.user_data["ai_tx_flow"])
        callback_update, query = self._make_callback_update(message, confirmation_from_reply(message, "ai:tx:ok"))
        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_get_active_account_by_id", new=AsyncMock(return_value=account)),

            patch.object(bot_main, "TransactionService", wraps=bot_main.TransactionService) as service_cls,
        ):
            await bot_main.ai_callback(callback_update, context)

        self.assertTrue(query.answered)
        service_cls.assert_called_once()
        self.assertNotIn("ai_tx_flow", context.user_data)
        self.assertEqual(len(conn.execute_calls), 2)
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("2600.00"))
        self.assertEqual(conn.drafts[5]["status"], "completed")
        self.assertIn("Операцію збережено", message.replies[-1]["text"])

    async def test_screenshot_amount_edit_updates_preview(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        context.user_data["ai_tx_flow"] = {
            "tx": {
                "date": "2026-05-03",
                "type": "expense",
                "amount": None,
                "currency": "UAH",
                "account_id": 11,
                "category_id": 31,
                "comment": "Супермаркет",
                "source": bot_main.AI_SCREENSHOT_SOURCE,
            },
            "accounts": {11: "Mono 1111"},
            "categories": {31: "Продукти"},
            "origin": bot_main.AI_SCREENSHOT_SOURCE,
            "draft_id": 5,
            "draft_source": bot_main.AI_SCREENSHOT_SOURCE,
        }
        update, message = self._make_photo_update()
        update.message.text = ""
        callback_update, query = self._make_callback_update(message, "ai:edit:amount")

        with patch.object(bot_main, "_pool", return_value=DummyPool(conn)):
            await bot_main.ai_callback(callback_update, context)

        self.assertTrue(query.answered)
        self.assertEqual(context.user_data["ai_tx_flow"]["await_field"], "amount")

        text_update = SimpleNamespace(effective_user=SimpleNamespace(id=123), message=DummyMessage(text="500 обід"))
        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_sync_ai_draft_flow", new=AsyncMock()),
        ):
            await bot_main.text_message(text_update, context)

        flow = context.user_data["ai_tx_flow"]
        self.assertNotIn("await_field", flow)
        self.assertEqual(flow["tx"]["amount"], Decimal("500"))
        self.assertEqual(flow["tx"]["comment"], "обід")
        self.assertIn("Перевірте чернетку", text_update.message.replies[-1]["text"])

    async def test_start_ai_tx_flow_without_amount_uses_catalog_categories_map(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_text_update("Приват продукти")
        parsed = SimpleNamespace(
            is_candidate_tx=True,
            intent="expense",
            amount=None,
            currency="UAH",
            currency_explicit=False,
            account_id="privatbank",
            category_id="groceries",
            comment="Приват продукти",
            date=date(2026, 5, 23),
            confidence=0.42,
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "parse_message_batch", return_value=[]),
            patch.object(bot_main, "parse_message", return_value=parsed),
        ):
            handled = await bot_main._start_ai_tx_flow(update, context, "Приват продукти", origin="voice")

        self.assertTrue(handled)
        self.assertIn("ai_tx_flow", context.user_data)
        self.assertTrue(context.user_data["ai_tx_flow"]["await_amount"])
        self.assertIn("Категорія", message.replies[-1]["text"])

    async def test_screenshot_confirm_expense_below_zero_auto_saves_with_credit_notice(self) -> None:
        account = {
            "id": 11,
            "tg_user_id": 123,
            "label": "Mono 1111",
            "currency": "UAH",
            "balance": Decimal("700.00"),
            "account_type": "main",
            "credit_limit": None,
            "monthly_interest_rate": None,
            "non_negative_account_type": "main",
        }
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        context.user_data["ai_tx_flow"] = {
            "tx": {
                "date": "2026-05-03",
                "type": "expense",
                "amount": Decimal("1000"),
                "currency": "UAH",
                "account_id": 11,
                "category_id": 31,
                "comment": "магазин",
                "source": bot_main.AI_SCREENSHOT_SOURCE,
            },
            "accounts": {11: "Mono 1111"},
            "categories": {31: "Продукти"},
            "origin": bot_main.AI_SCREENSHOT_SOURCE,
            "draft_id": 5,
            "draft_source": bot_main.AI_SCREENSHOT_SOURCE,
        }
        message = DummyMessage()
        conn.seed_draft(context.user_data["ai_tx_flow"])
        callback_update, query = self._make_callback_update(message, prepare_confirmation(context.user_data["ai_tx_flow"], "ai:tx:ok"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_get_active_account_by_id", new=AsyncMock(return_value=account)),

        ):
            await bot_main.ai_callback(callback_update, context)

        self.assertTrue(query.answered)
        self.assertNotIn("ai_tx_flow", context.user_data)
        self.assertEqual(len(conn.execute_calls), 3)
        self.assertEqual(conn.drafts[5]["status"], "completed")
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("-300.00"))
        self.assertEqual(conn.accounts_by_id[11]["account_type"], "credit")
        self.assertIsNone(conn.accounts_by_id[11]["credit_limit"])
        self.assertIn("кредитний режим", message.replies[-1]["text"])

    async def test_legacy_screenshot_credit_limit_text_input_still_retries_commit(self) -> None:
        account = {
            "id": 11,
            "tg_user_id": 123,
            "label": "Mono 1111",
            "currency": "UAH",
            "balance": Decimal("700.00"),
            "account_type": "main",
            "credit_limit": None,
            "monthly_interest_rate": None,
            "non_negative_account_type": "main",
        }
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        context.user_data["ai_tx_flow"] = {
            "tx": {
                "date": "2026-05-03",
                "type": "expense",
                "amount": Decimal("1000"),
                "currency": "UAH",
                "account_id": 11,
                "category_id": 31,
                "comment": "магазин",
                "source": bot_main.AI_SCREENSHOT_SOURCE,
            },
            "accounts": {11: "Mono 1111"},
            "categories": {31: "Продукти"},
            "origin": bot_main.AI_SCREENSHOT_SOURCE,
            "draft_id": 5,
            "draft_source": bot_main.AI_SCREENSHOT_SOURCE,
            "await_field": "credit_limit",
        }
        conn.seed_draft(context.user_data["ai_tx_flow"])
        update, message = self._make_text_update("1500")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_get_active_account_by_id", new=AsyncMock(return_value=account)),

        ):
            await bot_main.text_message(update, context)

        self.assertNotIn("ai_tx_flow", context.user_data)
        self.assertEqual(len(conn.execute_calls), 3)
        self.assertEqual(conn.drafts[5]["status"], "completed")
        self.assertIn("Операцію збережено", message.replies[-1]["text"])

    async def test_screenshot_credit_limit_exceeded_keeps_draft_editable(self) -> None:
        account = {
            "id": 11,
            "tg_user_id": 123,
            "label": "Mono 1111",
            "currency": "UAH",
            "balance": Decimal("50.00"),
            "account_type": "main",
            "credit_limit": Decimal("100.00"),
            "monthly_interest_rate": None,
            "non_negative_account_type": "main",
        }
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        context.user_data["ai_tx_flow"] = {
            "tx": {
                "date": "2026-05-03",
                "type": "expense",
                "amount": Decimal("200"),
                "currency": "UAH",
                "account_id": 11,
                "category_id": 31,
                "comment": "магазин",
                "source": bot_main.AI_SCREENSHOT_SOURCE,
            },
            "accounts": {11: "Mono 1111"},
            "categories": {31: "Продукти"},
            "origin": bot_main.AI_SCREENSHOT_SOURCE,
            "draft_id": 5,
            "draft_source": bot_main.AI_SCREENSHOT_SOURCE,
        }
        message = DummyMessage()
        conn.seed_draft(context.user_data["ai_tx_flow"])
        callback_update, query = self._make_callback_update(message, prepare_confirmation(context.user_data["ai_tx_flow"], "ai:tx:ok"))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_get_active_account_by_id", new=AsyncMock(return_value=account)),

        ):
            await bot_main.ai_callback(callback_update, context)

        self.assertTrue(query.answered)
        self.assertIn("ai_tx_flow", context.user_data)
        self.assertNotIn("await_field", context.user_data["ai_tx_flow"])
        self.assertEqual(len(conn.execute_calls), 0)
        self.assertEqual(conn.drafts[5]["status"], "pending")
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("50.00"))
        self.assertEqual(conn.draft_writes, [])
        self.assertIn("кредитний ліміт", message.replies[-1]["text"])
        self.assertNotIn("більше не активний", message.replies[-1]["text"])

    async def test_statement_screenshot_creates_batch_preview_and_confirms(self) -> None:
        account = {
            "id": 11,
            "tg_user_id": 123,
            "label": "Mono 1111",
            "currency": "UAH",
            "balance": Decimal("1000.00"),
            "account_type": "main",
            "credit_limit": None,
            "monthly_interest_rate": None,
            "non_negative_account_type": "main",
        }
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._statement_draft(), True)),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=self._statement_analysis())),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        self.assertIn("Знайшов витрати зі скріншота", message.replies[0]["text"])
        self.assertIn("Перевірте список операцій", message.replies[1]["text"])
        self.assertIn("Пропущено рядків", message.replies[1]["text"])
        self.assertIn("2026-05-04", message.replies[1]["text"])
        self.assertIn("ai_batch_tx_flow", context.user_data)
        self.assertNotIn("ai_tx_flow", context.user_data)
        self.assertEqual(conn.execute_calls, [])
        conn.seed_draft(context.user_data["ai_batch_tx_flow"])
        callback_update, query = self._make_callback_update(message, confirmation_from_reply(message, "ai:batch:ok"))
        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),

            patch.object(bot_main, "TransactionService", wraps=bot_main.TransactionService) as service_cls,
        ):
            await bot_main.ai_callback(callback_update, context)

        self.assertTrue(query.answered)
        self.assertNotIn("ai_batch_tx_flow", context.user_data)
        service_cls.assert_called_once()
        self.assertEqual(len(conn.execute_calls), 6)
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("800.00"))
        self.assertEqual(conn.drafts[5]["status"], "completed")
        self.assertIn("Операцій збережено: 2", message.replies[-1]["text"])

    async def test_statement_screenshot_preserves_explicit_try_currency_in_preview(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                11: {
                    "id": 11,
                    "tg_user_id": 123,
                    "label": "Готівка",
                    "currency": "UAH",
                    "balance": Decimal("1000.00"),
                    "account_type": "cash",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "cash",
                }
            }
        )
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._statement_draft(), True)),
        )
        analysis = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 1),
            type="expense",
            amount=None,
            currency="TRY",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.9,
            bank_name=None,
            card_last4=None,
            model="gpt-5.4-nano",
            detail="high",
            used_fallback=False,
            raw_response=None,
            mode="statement_expenses",
            dominant_currency="TRY",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("78.65"),
                    currency="TRY",
                    category_hint="market",
                    comment="ALANYA",
                    confidence=0.94,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("856.25"),
                    currency="TRY",
                    category_hint="groceries",
                    comment="BIM",
                    confidence=0.91,
                ),
            ),
            skipped_items_count=0,
            currency_evidence_kind="symbol",
            currency_evidence_text="₺",
            currency_resolution="evidence_override",
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=analysis)),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        flow = context.user_data["ai_batch_tx_flow"]
        self.assertIsNone(flow["account_id"])
        self.assertTrue(all(item["currency"] == "TRY" for item in flow["items"]))
        self.assertTrue(all(bool(item["currency_explicit"]) for item in flow["items"]))
        self.assertIn("TRY", message.replies[1]["text"])

    async def test_statement_screenshot_does_not_autopick_try_cash_account_from_currency_only(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                11: {
                    "id": 11,
                    "tg_user_id": 123,
                    "label": "Monobank",
                    "currency": "UAH",
                    "balance": Decimal("1000.00"),
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
                22: {
                    "id": 22,
                    "tg_user_id": 123,
                    "label": "Готівка",
                    "currency": "TRY",
                    "balance": Decimal("0.00"),
                    "account_type": "cash",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "cash",
                },
            }
        )
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._statement_draft(), True)),
        )
        catalog = {
            "accounts_rows": [(11, "Monobank"), (22, "Готівка")],
            "accounts_full": [conn.accounts_by_id[11], conn.accounts_by_id[22]],
            "expense_full": [(31, "Продукти", ["groceries"])],
            "income_full": [(41, "Зарплата", ["salary"])],
            "expense_rows": [(31, "Продукти")],
            "income_rows": [(41, "Зарплата")],
            "accounts_map": {11: "Monobank", 22: "Готівка"},
            "categories_map": {31: "Продукти", 41: "Зарплата"},
        }
        analysis = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 1),
            type="expense",
            amount=None,
            currency="TRY",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.9,
            bank_name=None,
            card_last4=None,
            model="gpt-5.4-nano",
            detail="high",
            used_fallback=False,
            raw_response=None,
            mode="statement_expenses",
            dominant_currency="TRY",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("78.65"),
                    currency="TRY",
                    category_hint="market",
                    comment="ALANYA",
                    confidence=0.94,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("856.25"),
                    currency="TRY",
                    category_hint="groceries",
                    comment="BIM",
                    confidence=0.91,
                ),
            ),
            skipped_items_count=0,
            currency_evidence_kind="symbol",
            currency_evidence_text="₺",
            currency_resolution="evidence_override",
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "USD"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=analysis)),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        flow = context.user_data["ai_batch_tx_flow"]
        self.assertIsNone(flow["account_id"])
        self.assertTrue(all(item["currency"] == "TRY" for item in flow["items"]))
        self.assertTrue(all(bool(item["currency_explicit"]) for item in flow["items"]))
        self.assertIn("TRY", message.replies[1]["text"])
        self.assertNotIn("Готівка", message.replies[1]["text"])

    async def test_statement_screenshot_confirm_opens_currency_resolution_menu_without_try_account(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                11: {
                    "id": 11,
                    "tg_user_id": 123,
                    "label": "Готівка",
                    "currency": "UAH",
                    "balance": Decimal("1000.00"),
                    "account_type": "cash",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "cash",
                }
            }
        )
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._statement_draft(), True)),
        )
        analysis = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 1),
            type="expense",
            amount=None,
            currency="TRY",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.9,
            bank_name=None,
            card_last4=None,
            model="gpt-5.4-nano",
            detail="high",
            used_fallback=False,
            raw_response=None,
            mode="statement_expenses",
            dominant_currency="TRY",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("78.65"),
                    currency="TRY",
                    category_hint="market",
                    comment="ALANYA",
                    confidence=0.94,
                ),
            ),
            skipped_items_count=0,
            currency_evidence_kind="symbol",
            currency_evidence_text="₺",
            currency_resolution="evidence_override",
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=analysis)),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        self.assertIsNone(context.user_data["ai_batch_tx_flow"]["account_id"])
        conn.seed_draft(context.user_data["ai_batch_tx_flow"])
        callback_update, query = self._make_callback_update(message, prepare_confirmation(context.user_data["ai_batch_tx_flow"], "ai:batch:ok"))
        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(
                bot_main,
                "_get_accounts_full",
                new=AsyncMock(return_value=[{"id": 11, "label": "Готівка", "currency": "UAH", "account_type": "cash"}]),
            ),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(11, "Готівка")])),
            patch.object(bot_main, "_sync_ai_batch_draft_flow", new=AsyncMock()),
        ):
            await bot_main.ai_callback(callback_update, context)

        self.assertTrue(query.answered)
        resolution_flow = context.user_data["currency_resolution_flow"]
        self.assertEqual(resolution_flow["mode"], "ai_batch")
        self.assertEqual(resolution_flow["source_currency"], "TRY")
        self.assertEqual(resolution_flow["target_account_id"], 11)
        self.assertEqual(resolution_flow["target_currency"], "UAH")
        self.assertIn("TRY", message.replies[-1]["text"])
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertTrue(any("Створити рахунок TRY" in label for label in labels))
        self.assertTrue(any("Автоматична конвертація" in label for label in labels))

    async def test_statement_screenshot_weak_symbol_default_marks_currency_non_explicit(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                11: {
                    "id": 11,
                    "tg_user_id": 123,
                    "label": "Готівка",
                    "currency": "UAH",
                    "balance": Decimal("1000.00"),
                    "account_type": "cash",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "cash",
                }
            }
        )
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._statement_draft(), True)),
        )
        analysis = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 2),
            type="expense",
            amount=None,
            currency="UAH",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.84,
            bank_name=None,
            card_last4=None,
            model="gpt-5.4-nano",
            detail="original",
            used_fallback=True,
            raw_response=None,
            mode="statement_expenses",
            dominant_currency="UAH",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 2),
                    type="expense",
                    amount=Decimal("78.65"),
                    currency="UAH",
                    category_hint="other",
                    comment="ALANYA",
                    confidence=0.90,
                ),
            ),
            skipped_items_count=0,
            currency_evidence_kind="symbol",
            currency_evidence_text="€",
            currency_resolution="weak_symbol_default",
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=analysis)),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        flow = context.user_data["ai_batch_tx_flow"]
        self.assertTrue(all(item["currency"] == "UAH" for item in flow["items"]))
        self.assertTrue(all(not bool(item["currency_explicit"]) for item in flow["items"]))
        self.assertIn("UAH", message.replies[1]["text"])

    async def test_statement_screenshot_unknown_currency_prefers_recent_uah_account_for_preview(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                45: {
                    "id": 45,
                    "tg_user_id": 123,
                    "label": "Monobank",
                    "currency": "UAH",
                    "balance": Decimal("-132.00"),
                    "account_type": "credit",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
                46: {
                    "id": 46,
                    "tg_user_id": 123,
                    "label": "ПриватБанк",
                    "currency": "UAH",
                    "balance": Decimal("0.00"),
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
                47: {
                    "id": 47,
                    "tg_user_id": 123,
                    "label": "Готівка",
                    "currency": "TRY",
                    "balance": Decimal("0.00"),
                    "account_type": "cash",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "cash",
                },
                48: {
                    "id": 48,
                    "tg_user_id": 123,
                    "label": "USDT",
                    "currency": "USD",
                    "balance": Decimal("0.00"),
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
            },
            recent_categorized_transactions=[
                {"category_id": 242, "account_id": 45, "comment": "apple"},
            ],
        )
        context = self._make_context()
        update, message = self._make_photo_update()
        catalog = {
            "accounts_rows": [(45, "Monobank"), (46, "ПриватБанк"), (47, "Готівка"), (48, "USDT")],
            "accounts_full": [
                conn.accounts_by_id[45],
                conn.accounts_by_id[46],
                conn.accounts_by_id[47],
                conn.accounts_by_id[48],
            ],
            "expense_full": [(231, "Продукти", ["bim"]), (239, "Здоров'я", ["eczane"]), (242, "Підписки та сервіси", ["apple"])],
            "income_full": [(41, "Зарплата", ["зарплата"])],
            "expense_rows": [(231, "Продукти"), (239, "Здоров'я"), (242, "Підписки та сервіси")],
            "income_rows": [(41, "Зарплата")],
            "accounts_map": {45: "Monobank", 46: "ПриватБанк", 47: "Готівка", 48: "USDT"},
            "categories_map": {231: "Продукти", 239: "Здоров'я", 242: "Підписки та сервіси", 41: "Зарплата"},
        }
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._statement_draft(), True)),
        )
        analysis = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 1),
            type="expense",
            amount=None,
            currency="TRY",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.72,
            bank_name=None,
            card_last4=None,
            model="gpt-5.4-nano",
            detail="original",
            used_fallback=True,
            raw_response=None,
            mode="statement_expenses",
            dominant_currency="TRY",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("78.65"),
                    currency="TRY",
                    category_hint=None,
                    comment="ALANYA",
                    confidence=0.90,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("132.86"),
                    currency="TRY",
                    category_hint=None,
                    comment="Apple",
                    confidence=0.92,
                ),
            ),
            skipped_items_count=0,
            currency_evidence_kind="unknown",
            currency_evidence_text="",
            currency_resolution=None,
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "USD"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=analysis)),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        flow = context.user_data["ai_batch_tx_flow"]
        self.assertEqual(flow["account_id"], 45)
        self.assertTrue(all(item["currency"] == "UAH" for item in flow["items"]))
        self.assertTrue(all(not bool(item["currency_explicit"]) for item in flow["items"]))
        self.assertIn("Monobank", message.replies[1]["text"])
        self.assertIn("UAH", message.replies[1]["text"])
        self.assertNotIn("TRY", message.replies[1]["text"])

    async def test_statement_screenshot_symbol_only_try_without_resolution_prefers_recent_uah_account_for_preview(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                45: {
                    "id": 45,
                    "tg_user_id": 123,
                    "label": "Monobank",
                    "currency": "UAH",
                    "balance": Decimal("-132.00"),
                    "account_type": "credit",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
                46: {
                    "id": 46,
                    "tg_user_id": 123,
                    "label": "ПриватБанк",
                    "currency": "UAH",
                    "balance": Decimal("0.00"),
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                },
                47: {
                    "id": 47,
                    "tg_user_id": 123,
                    "label": "Готівка",
                    "currency": "TRY",
                    "balance": Decimal("0.00"),
                    "account_type": "cash",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "cash",
                },
            },
            recent_categorized_transactions=[
                {"category_id": 242, "account_id": 45, "comment": "apple"},
            ],
        )
        context = self._make_context()
        update, message = self._make_photo_update()
        catalog = {
            "accounts_rows": [(45, "Monobank"), (46, "ПриватБанк"), (47, "Готівка")],
            "accounts_full": [
                conn.accounts_by_id[45],
                conn.accounts_by_id[46],
                conn.accounts_by_id[47],
            ],
            "expense_full": [(231, "Продукти", ["bim"]), (239, "Здоров'я", ["eczane"]), (242, "Підписки та сервіси", ["apple"])],
            "income_full": [(41, "Зарплата", ["зарплата"])],
            "expense_rows": [(231, "Продукти"), (239, "Здоров'я"), (242, "Підписки та сервіси")],
            "income_rows": [(41, "Зарплата")],
            "accounts_map": {45: "Monobank", 46: "ПриватБанк", 47: "Готівка"},
            "categories_map": {231: "Продукти", 239: "Здоров'я", 242: "Підписки та сервіси", 41: "Зарплата"},
        }
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._statement_draft(), True)),
        )
        analysis = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 3),
            type="expense",
            amount=None,
            currency="TRY",
            account_hint="monobank",
            category_hint=None,
            comment="statement",
            confidence=0.86,
            bank_name="monobank",
            card_last4=None,
            model="gpt-5.4-nano",
            detail="original",
            used_fallback=True,
            raw_response=None,
            mode="statement_expenses",
            dominant_currency="TRY",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 3),
                    type="expense",
                    amount=Decimal("252.88"),
                    currency="TRY",
                    category_hint=None,
                    comment="OZDE KASAP",
                    confidence=0.91,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 3),
                    type="expense",
                    amount=Decimal("164.85"),
                    currency="TRY",
                    category_hint=None,
                    comment="Apple",
                    confidence=0.92,
                ),
            ),
            skipped_items_count=2,
            currency_evidence_kind="symbol",
            currency_evidence_text="₺",
            currency_resolution=None,
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "USD"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=analysis)),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        flow = context.user_data["ai_batch_tx_flow"]
        self.assertEqual(flow["account_id"], 45)
        self.assertTrue(all(item["currency"] == "UAH" for item in flow["items"]))
        self.assertTrue(all(not bool(item["currency_explicit"]) for item in flow["items"]))
        self.assertIn("Monobank", message.replies[1]["text"])
        self.assertIn("UAH", message.replies[1]["text"])
        self.assertNotIn("TRY", message.replies[1]["text"])

    async def test_statement_screenshot_uses_recent_history_for_account_and_category_autopick(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                11: {"id": 11, "label": "Cash", "currency": "UAH", "balance": Decimal("50.00")},
                22: {"id": 22, "label": "Card", "currency": "UAH", "balance": Decimal("500.00")},
            },
            recent_account_id_by_currency={"UAH": 22},
            recent_categorized_transactions=[
                {"category_id": 32, "account_id": 22, "comment": "uber trip downtown"},
            ],
        )
        context = self._make_context()
        update, message = self._make_photo_update()
        catalog = {
            "accounts_rows": [(11, "Cash"), (22, "Card")],
            "accounts_full": [
                {"id": 11, "label": "Cash", "currency": "UAH", "balance": Decimal("50.00"), "account_type": "cash"},
                {"id": 22, "label": "Card", "currency": "UAH", "balance": Decimal("500.00"), "account_type": "card"},
            ],
            "expense_full": [(31, "Продукти", ["магазин"]), (32, "Транспорт", ["таксі"])],
            "income_full": [(41, "Зарплата", ["зарплата"])],
            "expense_rows": [(31, "Продукти"), (32, "Транспорт")],
            "income_rows": [(41, "Зарплата")],
            "accounts_map": {11: "Cash", 22: "Card"},
            "categories_map": {31: "Продукти", 32: "Транспорт", 41: "Зарплата"},
        }
        analysis = ScreenshotAnalysis(
            transaction_date=date(2026, 5, 3),
            type="expense",
            amount=None,
            currency="UAH",
            account_hint=None,
            category_hint=None,
            comment="Виписка",
            confidence=0.9,
            bank_name=None,
            card_last4=None,
            model="gpt-5.4-nano",
            detail="high",
            used_fallback=False,
            raw_response=None,
            mode="statement_expenses",
            dominant_currency="UAH",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 5, 3),
                    type="expense",
                    amount=Decimal("250.00"),
                    currency="UAH",
                    category_hint=None,
                    comment="uber trip",
                    confidence=0.92,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 5, 4),
                    type="expense",
                    amount=Decimal("100.00"),
                    currency="UAH",
                    category_hint="магазин",
                    comment="АТБ",
                    confidence=0.92,
                ),
            ),
            skipped_items_count=0,
        )
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=None),
            create_or_get=AsyncMock(return_value=(self._statement_draft(), True)),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=analysis)),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        flow = context.user_data["ai_batch_tx_flow"]
        self.assertEqual(flow["account_id"], 22)
        self.assertEqual(flow["items"][0]["category_id"], 32)
        self.assertIn("Card", message.replies[1]["text"])
        self.assertIn("Транспорт", message.replies[1]["text"])

    async def test_get_categories_full_merges_canonical_aliases_for_screenshot_text_matching(self) -> None:
        conn = SimpleNamespace(
            fetch=AsyncMock(
                return_value=[
                    {"id": 31, "name": "Підписки та сервіси", "slug": "subscriptions_services", "aliases": []},
                    {"id": 32, "name": "Здоров'я", "slug": "health", "aliases": []},
                    {"id": 33, "name": "Продукти", "slug": "groceries", "aliases": []},
                    {"id": 35, "name": "Кафе, ресторани, доставка", "slug": "cafes_restaurants_delivery", "aliases": []},
                    {"id": 34, "name": "Інше", "slug": "other", "aliases": []},
                ]
            )
        )
        scope = SimpleNamespace(is_family=False, family_id=None)

        with patch.object(bot_main, "_get_finance_scope", AsyncMock(return_value=scope)):
            categories = await bot_main._get_categories_full(conn, 123, "expense")

        self.assertEqual(bot_main._pick_category_id_by_text("Apple", categories), 31)
        self.assertEqual(bot_main._pick_category_id_by_text("VITAMIN ECZANESI", categories), 32)
        self.assertEqual(bot_main._pick_category_id_by_text("BiM", categories), 33)
        self.assertEqual(bot_main._pick_category_id_by_normalization(["market", "Glovo Silpo"], categories), 33)
        self.assertEqual(bot_main._pick_category_id_by_normalization([None, "Glovo sushi"], categories), 35)
        self.assertEqual(bot_main._pick_category_id_by_normalization([None, "Apple"], categories), 31)
        self.assertEqual(bot_main._pick_category_id_by_normalization([None, "VITAMIN ECZANESI"], categories), 32)

    async def test_duplicate_pending_statement_screenshot_restores_batch_preview(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(get_by_media_key=AsyncMock(return_value=self._statement_draft()))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock()) as analyze_mock,
        ):
            await bot_main.photo_message(update, context)

        analyze_mock.assert_not_called()
        self.assertTrue(message.replies[0]["text"])
        self.assertIn("Перевірте список операцій", message.replies[1]["text"])
        self.assertIn("2026-05-04", message.replies[1]["text"])
        self.assertIn("ai_batch_tx_flow", context.user_data)

    async def test_duplicate_pending_statement_screenshot_restores_dominant_currency_for_legacy_items(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        legacy_draft = self._draft(
            metadata={
                "mode": "statement_expenses",
                "model": "gpt-5.4-nano",
                "screenshot_draft_schema_version": bot_main.AI_SCREENSHOT_DRAFT_SCHEMA_VERSION,
                "dominant_currency": "TRY",
                "statement_items": [
                    {
                        "date": "2026-06-01",
                        "type": "expense",
                        "amount": "78.65",
                        "currency": "EUR",
                        "category_id": 31,
                        "comment": "ALANYA",
                        "source": bot_main.AI_SCREENSHOT_SOURCE,
                    },
                    {
                        "date": "2026-06-01",
                        "type": "expense",
                        "amount": "856.25",
                        "currency": "EUR",
                        "category_id": 31,
                        "comment": "BIM",
                        "source": bot_main.AI_SCREENSHOT_SOURCE,
                    },
                ],
                "statement_skipped_items_count": 0,
            }
        )
        draft_service = SimpleNamespace(get_by_media_key=AsyncMock(return_value=legacy_draft))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock()) as analyze_mock,
        ):
            await bot_main.photo_message(update, context)

        analyze_mock.assert_not_called()
        flow = context.user_data["ai_batch_tx_flow"]
        self.assertTrue(all(item["currency"] == "TRY" for item in flow["items"]))
        self.assertTrue(all(bool(item["currency_explicit"]) for item in flow["items"]))

    async def test_duplicate_pending_statement_screenshot_keeps_non_explicit_items_editable(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        draft = self._draft(
            metadata={
                "mode": "statement_expenses",
                "model": "gpt-5.4-nano",
                "screenshot_draft_schema_version": bot_main.AI_SCREENSHOT_DRAFT_SCHEMA_VERSION,
                "dominant_currency": "UAH",
                "statement_items": [
                    {
                        "date": "2026-06-01",
                        "type": "expense",
                        "amount": "78.65",
                        "currency": "TRY",
                        "currency_explicit": False,
                        "category_id": 31,
                        "comment": "ALANYA",
                        "source": bot_main.AI_SCREENSHOT_SOURCE,
                    }
                ],
                "statement_skipped_items_count": 0,
            }
        )
        draft_service = SimpleNamespace(get_by_media_key=AsyncMock(return_value=draft))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock()) as analyze_mock,
        ):
            await bot_main.photo_message(update, context)

        analyze_mock.assert_not_called()
        flow = context.user_data["ai_batch_tx_flow"]
        self.assertEqual(flow["items"][0]["currency"], "UAH")
        self.assertFalse(bool(flow["items"][0]["currency_explicit"]))
        self.assertIn("UAH", message.replies[1]["text"])

    async def test_legacy_pending_statement_screenshot_is_reanalyzed_and_refreshes_draft(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        legacy_draft = self._draft(
            metadata={
                "mode": "statement_expenses",
                "model": "gpt-5.4-nano",
                "dominant_currency": "EUR",
                "statement_items": [
                    {
                        "date": "2026-06-02",
                        "type": "expense",
                        "amount": "78.65",
                        "currency": "EUR",
                        "category_id": 31,
                        "comment": "ALANYA",
                        "source": bot_main.AI_SCREENSHOT_SOURCE,
                    },
                    {
                        "date": "2026-06-02",
                        "type": "expense",
                        "amount": "856.25",
                        "currency": "EUR",
                        "category_id": 33,
                        "comment": "BiM",
                        "source": bot_main.AI_SCREENSHOT_SOURCE,
                    },
                ],
                "statement_skipped_items_count": 0,
            }
        )
        refreshed_draft = self._statement_draft()
        draft_service = SimpleNamespace(
            get_by_media_key=AsyncMock(return_value=legacy_draft),
            update_draft=AsyncMock(return_value=refreshed_draft),
            create_or_get=AsyncMock(),
        )

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(return_value=self._statement_analysis())) as analyze_mock,
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        analyze_mock.assert_awaited_once()
        draft_service.update_draft.assert_awaited_once()
        draft_service.create_or_get.assert_not_called()
        flow = context.user_data["ai_batch_tx_flow"]
        self.assertTrue(all(item["currency"] == "UAH" for item in flow["items"]))
        self.assertEqual(flow["draft_id"], refreshed_draft.id)
        self.assertTrue(message.replies[0]["text"])
        self.assertIn("UAH", message.replies[1]["text"])

    async def test_statement_batch_item_date_edit_updates_only_selected_item(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        context.user_data["ai_batch_tx_flow"] = bot_main._build_ai_batch_tx_flow_state(
            items=[
                {"date": "2026-05-03", "type": "expense", "amount": Decimal("120"), "currency": "UAH", "category_id": 31, "comment": "АТБ", "source": bot_main.AI_SCREENSHOT_SOURCE},
                {"date": "2026-05-04", "type": "expense", "amount": Decimal("80"), "currency": "UAH", "category_id": 31, "comment": "Сільпо", "source": bot_main.AI_SCREENSHOT_SOURCE},
            ],
            account_id=11,
            accounts_map={11: "Mono 1111"},
            categories_map={31: "Продукти"},
            origin=bot_main.AI_SCREENSHOT_SOURCE,
            raw_text="statement",
        )

        prompt_message = DummyMessage()
        callback_update, _query = self._make_callback_update(prompt_message, "ai:batch:item:1:date")
        with patch.object(bot_main, "_pool", return_value=DummyPool(conn)):
            await bot_main.ai_callback(callback_update, context)

        self.assertEqual(context.user_data["ai_batch_tx_flow"]["await_field"], "date")
        self.assertEqual(context.user_data["ai_batch_tx_flow"]["edit_item_index"], 1)

        text_update, reply_message = self._make_text_update("2026-05-10")
        handled = await bot_main._handle_ai_batch_tx_text_input(
            text_update,
            context,
            context.user_data["ai_batch_tx_flow"],
        )

        self.assertTrue(handled)
        items = context.user_data["ai_batch_tx_flow"]["items"]
        self.assertEqual(items[0]["date"], "2026-05-03")
        self.assertEqual(items[1]["date"], "2026-05-10")
        self.assertIn("2026-05-10", reply_message.replies[-1]["text"])

    async def test_duplicate_completed_screenshot_is_not_reprocessed(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(get_by_media_key=AsyncMock(return_value=self._draft(status="completed")))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock()) as analyze_mock,
        ):
            await bot_main.photo_message(update, context)

        analyze_mock.assert_not_called()
        self.assertIn("уже був підтверджений", message.replies[-1]["text"])

    async def test_duplicate_pending_screenshot_restores_preview(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(get_by_media_key=AsyncMock(return_value=self._draft()))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock()) as analyze_mock,
        ):
            await bot_main.photo_message(update, context)

        analyze_mock.assert_not_called()
        self.assertEqual(context.user_data["ai_tx_flow"]["draft_id"], 5)
        self.assertIn("already" if False else "уже є в чернетках", message.replies[0]["text"])

    async def test_screenshot_openai_error_returns_user_message(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(get_by_media_key=AsyncMock(return_value=None))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock(side_effect=OpenAIVisionError("vision down"))),
            patch.object(bot_main, "notify_admins", new=AsyncMock()),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        self.assertIn("Не вдалося розібрати скріншот", message.replies[-1]["text"])

    async def test_screenshot_invalid_json_returns_user_message(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(get_by_media_key=AsyncMock(return_value=None))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(
                bot_main,
                "analyze_screenshot",
                new=AsyncMock(side_effect=OpenAIVisionInvalidResponseError("bad json")),
            ),
            patch.object(bot_main, "notify_admins", new=AsyncMock()),
            patch.object(bot_main.config, "OPENAI_API_KEY", "test-key"),
        ):
            await bot_main.photo_message(update, context)

        self.assertIn("Не вдалося розібрати скріншот", message.replies[-1]["text"])

    async def test_screenshot_missing_api_key_short_circuits(self) -> None:
        conn = DummyConn()
        context = self._make_context()
        update, message = self._make_photo_update()
        draft_service = SimpleNamespace(get_by_media_key=AsyncMock(return_value=None))

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=self._catalog())),
            patch.object(bot_main, "_get_user", new=AsyncMock(return_value={"base_currency": "UAH"})),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
            patch.object(bot_main.config, "OPENAI_API_KEY", None),
            patch.object(bot_main, "analyze_screenshot", new=AsyncMock()) as analyze_mock,
        ):
            await bot_main.photo_message(update, context)

        analyze_mock.assert_not_called()
        self.assertIn("Розпізнавання скріншотів зараз недоступне", message.replies[-1]["text"])

    async def test_billing_callback_restores_draft_and_commits(self) -> None:
        account = {
            "id": 11,
            "tg_user_id": 123,
            "label": "Mono 1111",
            "currency": "UAH",
            "balance": Decimal("1000.00"),
            "account_type": "main",
            "credit_limit": None,
            "monthly_interest_rate": None,
            "non_negative_account_type": "main",
        }
        conn = DummyConn(accounts_by_id={11: account})
        context = self._make_context()
        message = DummyMessage()
        callback_update, query = self._make_callback_update(message, "btx:8:edit:back")
        draft_service = SimpleNamespace(
            get_by_id=AsyncMock(return_value=self._billing_draft(draft_id=8, account_id=11, category_id=51)),
            update_draft=AsyncMock(return_value=None),
            mark_completed=AsyncMock(),
            mark_cancelled=AsyncMock(),
        )
        catalog = {
            "accounts_rows": [(11, "Mono 1111")],
            "accounts_full": [{"id": 11, "label": "Mono 1111", "currency": "UAH", "balance": Decimal("1000.00")}],
            "expense_full": [(51, "Підписки та сервіси", ["subscriptions"])],
            "income_full": [(41, "Зарплата", ["salary"])],
            "expense_rows": [(51, "Підписки та сервіси")],
            "income_rows": [(41, "Зарплата")],
            "accounts_map": {11: "Mono 1111"},
            "categories_map": {51: "Підписки та сервіси"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_mark_daily_expense_recorded_from_result", new=AsyncMock()),
            patch.object(bot_main, "_maybe_prompt_saving_after_income", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "_find_existing_transaction_by_source", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
        ):
            await bot_main.billing_tx_callback(callback_update, context)
            self.assertEqual(conn.execute_calls, [])
            conn.seed_draft(context.user_data["ai_tx_flow"])
            action = f"btx:{context.user_data['ai_tx_flow']['draft_id']}:tx:ok"
            confirmation_update, _ = self._make_callback_update(message, confirmation_from_reply(message, action))
            with patch.object(bot_main, "AiTransactionDraftService", AiTransactionDraftService):
                await bot_main.billing_tx_callback(confirmation_update, context)

        self.assertTrue(query.answered)
        draft_service.get_by_id.assert_awaited_once_with(8, 123)
        self.assertEqual(conn.drafts[8]["status"], "completed")
        self.assertNotIn("ai_tx_flow", context.user_data)
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("501.00"))
        self.assertIn("Операцію збережено", message.replies[-1]["text"])

    async def test_billing_callback_without_account_prompts_prefixed_picker(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                11: {"id": 11, "label": "Mono 1111", "currency": "UAH", "balance": Decimal("50.00")},
                12: {"id": 12, "label": "Cash", "currency": "UAH", "balance": Decimal("70.00")},
            }
        )
        context = self._make_context()
        message = DummyMessage()
        callback_update, query = self._make_callback_update(message, "btx:9:edit:back")
        draft_service = SimpleNamespace(
            get_by_id=AsyncMock(return_value=self._billing_draft(draft_id=9, account_id=None, category_id=51)),
            update_draft=AsyncMock(return_value=None),
            mark_completed=AsyncMock(),
            mark_cancelled=AsyncMock(),
        )
        catalog = {
            "accounts_rows": [(11, "Mono 1111"), (12, "Cash")],
            "accounts_full": [
                {"id": 11, "label": "Mono 1111", "currency": "UAH", "balance": Decimal("50.00")},
                {"id": 12, "label": "Cash", "currency": "UAH", "balance": Decimal("70.00")},
            ],
            "expense_full": [(51, "Підписки та сервіси", ["subscriptions"])],
            "income_full": [(41, "Зарплата", ["salary"])],
            "expense_rows": [(51, "Підписки та сервіси")],
            "income_rows": [(41, "Зарплата")],
            "accounts_map": {11: "Mono 1111", 12: "Cash"},
            "categories_map": {51: "Підписки та сервіси"},
        }

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_load_ai_catalog", new=AsyncMock(return_value=catalog)),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(11, "Mono 1111"), (12, "Cash")])),
            patch.object(bot_main, "_find_existing_transaction_by_source", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "AiTransactionDraftService", return_value=draft_service),
        ):
            await bot_main.billing_tx_callback(callback_update, context)
            self.assertEqual(conn.execute_calls, [])
            conn.seed_draft(context.user_data["ai_tx_flow"])
            action = f"btx:{context.user_data['ai_tx_flow']['draft_id']}:tx:ok"
            confirmation_update, _ = self._make_callback_update(message, confirmation_from_reply(message, action))
            with patch.object(bot_main, "AiTransactionDraftService", AiTransactionDraftService):
                await bot_main.billing_tx_callback(confirmation_update, context)

        self.assertTrue(query.answered)
        self.assertIn("ai_tx_flow", context.user_data)
        self.assertIsNone(context.user_data["ai_tx_flow"]["tx"]["account_id"])
        self.assertIn("Оберіть рахунок", message.replies[-1]["text"])
        rows = message.replies[-1]["reply_markup"].args[0]
        callback_data = [button.kwargs.get("callback_data") for row in rows for button in row]
        self.assertIn("btx:9:pick:acct:11", callback_data)
        self.assertEqual(conn.drafts[9]["status"], "pending")


if __name__ == "__main__":
    unittest.main()
