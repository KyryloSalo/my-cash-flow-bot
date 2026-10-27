"""Replay the real Telegram confirmation handlers without provider or DB access."""
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from tests import test_screenshot_flow as fixtures
from tests.financial_fixtures import prepare_confirmation
from screenshot_review import unresolved_photo_currency

bot_main = fixtures.bot_main


class PhotoCurrencyConfirmationTests(unittest.IsolatedAsyncioTestCase):
    def flow(self, finding):
        return {
            "origin": "screenshot", "draft_source": "screenshot",
            "draft_metadata": {"needs_review": True, "field_findings": [finding], "currency_candidates": ["UAH", "EUR"]},
            "tx": {"type": "expense", "amount": Decimal("100"), "currency": "UAH", "currency_explicit": True, "account_id": 11, "category_id": 31, "date": "2026-10-27"},
            "items": [{"type": "expense", "amount": Decimal("100"), "currency": "UAH", "currency_explicit": True}],
            "account_id": 11,
        }

    async def test_current_single_and_batch_callbacks_cannot_write_unresolved_currency(self):
        harness = fixtures.ScreenshotFlowTests()
        for mode in ("single", "batch"):
            for finding in ("currency_unknown", "currency_conflict", "date_unknown", "amount_conflict", "type_conflict", "status_conflict"):
                with self.subTest(mode=mode, finding=finding):
                    flow = self.flow(finding)
                    context = harness._make_context()
                    key = "ai_tx_flow" if mode == "single" else "ai_batch_tx_flow"
                    action = "ai:tx:ok" if mode == "single" else "ai:batch:ok"
                    context.user_data[key] = flow
                    message = fixtures.DummyMessage()
                    update, _ = harness._make_callback_update(message, prepare_confirmation(flow, action))
                    conn = fixtures.DummyConn()
                    with (
                        patch.object(bot_main, "_pool", return_value=fixtures.DummyPool(conn)),
                        patch.object(bot_main, "TransactionService") as writes,
                        patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
                    ):
                        await bot_main.ai_callback(update, context)
                    writes.assert_not_called()
                    self.assertEqual(conn.execute_calls, [])
                    expected = "валюту не визначено однозначно" if finding.startswith("currency_") else "потрібне уточнення даних"
                    self.assertIn(expected, message.replies[-1]["text"])
                    self.assertIs(context.user_data[key], flow)

    async def test_invalid_callbacks_cannot_write_screenshot_transactions(self):
        harness = fixtures.ScreenshotFlowTests()
        for mode in ("single", "batch"):
            for variant in ("unsigned", "forged_nonce", "stale_nonce", "changed_amount", "wrong_action"):
                with self.subTest(mode=mode, variant=variant):
                    flow = self.flow("currency_conflict")
                    # Use an otherwise resolved screenshot so the currency gate
                    # cannot hide a failure of callback validation itself.
                    flow["draft_metadata"] = {}
                    context = harness._make_context()
                    key = "ai_tx_flow" if mode == "single" else "ai_batch_tx_flow"
                    action = "ai:tx:ok" if mode == "single" else "ai:batch:ok"
                    context.user_data[key] = flow
                    token = prepare_confirmation(flow, action)
                    if variant == "unsigned":
                        token = action
                    elif variant == "forged_nonce":
                        prefix, nonce = token.rsplit(":pv:", 1)
                        replacement = "A" if nonce[0] != "A" else "B"
                        token = prefix + ":pv:" + replacement + nonce[1:]
                    elif variant == "stale_nonce":
                        prepare_confirmation(flow, action)
                    elif variant == "changed_amount":
                        flow["tx"]["amount"] = Decimal("101")
                        flow["items"][0]["amount"] = Decimal("101")
                    else:
                        other = "ai:batch:ok" if mode == "single" else "ai:tx:ok"
                        token = token.replace(action, other, 1)
                    message = fixtures.DummyMessage()
                    update, _ = harness._make_callback_update(message, token)
                    conn = fixtures.DummyConn()
                    with (
                        patch.object(bot_main, "_pool", return_value=fixtures.DummyPool(conn)),
                        patch.object(bot_main, "TransactionService") as writes,
                        patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
                    ):
                        await bot_main.ai_callback(update, context)
                    writes.assert_not_called()
                    self.assertEqual(conn.execute_calls, [])
                    self.assertIs(context.user_data[key], flow)

    def test_unresolved_cards_do_not_offer_confirmation(self):
        flow = self.flow("currency_conflict")
        for markup in (bot_main._ai_tx_confirm_markup(flow), bot_main._ai_batch_confirm_markup(flow, 1)):
            callbacks = [str(button.callback_data) for row in markup.inline_keyboard for button in row]
            self.assertFalse(any(":ok" in callback for callback in callbacks))

    def test_missing_currency_without_metadata_is_not_positive_resolution(self):
        flow = self.flow("currency_unknown")
        flow.pop("draft_metadata")
        flow["tx"]["currency"] = ""
        self.assertTrue(unresolved_photo_currency(flow))

    def test_photo_batch_builder_preserves_unknowns_and_row_findings(self):
        flow = bot_main._build_ai_batch_tx_flow_state(
            items=[{"type": "expense", "amount": None, "date": "", "currency": "", "field_findings": ["date_unknown"]}],
            account_id=None, accounts_map={}, categories_map={}, origin="screenshot", raw_text="synthetic",
        )
        self.assertEqual(flow["items"][0]["date"], "")
        self.assertEqual(flow["items"][0]["currency"], "")
        self.assertIn("date_unknown", flow["items"][0]["field_findings"])

    def test_clean_photo_and_manual_text_keep_normal_confirmation(self):
        flow = self.flow("currency_unknown")
        flow["draft_metadata"] = {}
        self.assertFalse(unresolved_photo_currency(flow))
        flow["origin"] = flow["draft_source"] = "text"
        flow["tx"]["currency"] = ""
        self.assertFalse(unresolved_photo_currency(flow))
