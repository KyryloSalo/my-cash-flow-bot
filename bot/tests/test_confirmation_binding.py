from __future__ import annotations

from decimal import Decimal
import importlib
import importlib.util
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # noqa: E402


class ConfirmationBindingTests(unittest.TestCase):
    def guard(self):
        self.assertIsNotNone(importlib.util.find_spec("confirmation_guard"), "Financial preview binding must be implemented")
        return importlib.import_module("confirmation_guard")

    def bind(self, flow, action="expense:confirm"):
        guard = self.guard()
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("Confirm", callback_data=action)], [InlineKeyboardButton("Cancel", callback_data="expense:cancel")]])
        bound = guard.bind_financial_preview(flow, markup)
        return bound.inline_keyboard[0][0].callback_data, bound

    def test_old_message_does_not_authorize_new_draft(self):
        old = {"amount": Decimal("50"), "account_id": 1, "kind": "expense"}
        old_data, _ = self.bind(old)
        new = {"amount": Decimal("500"), "account_id": 1, "kind": "expense"}
        new_data, _ = self.bind(new)
        guard = self.guard()
        self.assertFalse(guard.is_current_confirmation(new, old_data))
        self.assertTrue(guard.is_current_confirmation(new, new_data))
        self.assertFalse(guard.is_current_confirmation({}, new_data))

    def test_editing_any_draft_payload_requires_a_new_preview(self):
        for action, flow, change in (
            ("expense:confirm", {"amount": Decimal("50")}, lambda f: f.update(amount=Decimal("500"))),
            ("transfer:confirm", {"fx_rate": Decimal("40")}, lambda f: f.update(fx_rate=Decimal("45"))),
            ("ai:tx:confirm", {"tx": {"account_id": 1, "amount": 50}}, lambda f: f["tx"].update(account_id=2)),
            ("saving:topup:confirm:ok", {"target_account_id": 1}, lambda f: f.update(target_account_id=2)),
            ("debt:confirm", {"counterparty_name": "A"}, lambda f: f.update(counterparty_name="B")),
        ):
            with self.subTest(action=action):
                data, _ = self.bind(flow, action)
                self.assertTrue(self.guard().is_current_confirmation(flow, data))
                change(flow)
                self.assertFalse(self.guard().is_current_confirmation(flow, data))

    def test_rendering_a_new_preview_invalidates_the_previous_version(self):
        flow = {"amount": Decimal("50")}
        first, _ = self.bind(flow)
        second, _ = self.bind(flow)
        self.assertFalse(self.guard().is_current_confirmation(flow, first))
        self.assertTrue(self.guard().is_current_confirmation(flow, second))

    def test_valid_confirmation_is_consumed_exactly_once_at_commit_boundary(self):
        flow = {"amount": Decimal("50"), "account_id": 1, "kind": "expense"}
        data, _ = self.bind(flow)
        guard = self.guard()

        self.assertEqual(guard.validated_confirmation_action(flow, data), "expense:confirm")
        self.assertTrue(guard.is_current_confirmation(flow, data))
        self.assertTrue(guard.consume_financial_confirmation(flow, data))
        self.assertFalse(guard.consume_financial_confirmation(flow, data))
        self.assertFalse(guard.is_current_confirmation(flow, data))

    def test_unsigned_tampered_or_different_action_is_rejected(self):
        flow = {"amount": Decimal("50")}
        data, _ = self.bind(flow)
        for invalid in ("expense:confirm", data + "x", data + "я", data.replace("expense:", "income:"), None):
            with self.subTest(data=invalid):
                try:
                    valid = self.guard().is_current_confirmation(flow, invalid)
                except (TypeError, ValueError) as exc:
                    self.fail(f"Malformed callback must be rejected, not raise: {exc}")
                self.assertFalse(valid)

    def test_navigation_is_unchanged_and_callbacks_fit_telegram_limit(self):
        flow = {"amount": Decimal("50")}
        data, markup = self.bind(flow, "saving:topup:confirm:ok")
        self.assertLessEqual(len(data.encode("utf8")), 64)
        self.assertEqual(markup.inline_keyboard[1][0].callback_data, "expense:cancel")
        self.assertEqual(markup.inline_keyboard[0][0].text, "Confirm")

    def test_actual_ai_actions_require_current_preview_and_normalize_after_validation(self):
        for action in ("ai:tx:ok", "ai:batch:ok", "btx:17:tx:ok", "saving:confirm:ok", "debt:confirm"):
            with self.subTest(action=action):
                flow = {"draft_id": 17, "amount": Decimal("50")}
                data, _ = self.bind(flow, action)
                guard = self.guard()
                self.assertEqual(guard.validated_confirmation_action(flow, data), action)
                self.assertIsNone(guard.validated_confirmation_action(flow, action))
                flow["amount"] = Decimal("500")
                self.assertIsNone(guard.validated_confirmation_action(flow, data))
                self.assertEqual(guard.validated_confirmation_action(flow, "saving:cancel"), "saving:cancel")

    def test_billing_route_keeps_binding_when_forwarded_to_ai_handler(self):
        flow = {"draft_id": 17, "tx": {"amount": 50}}
        data, _ = self.bind(flow, "btx:17:tx:confirm")
        self.assertTrue(self.guard().is_current_confirmation(flow, data))
        self.assertTrue(self.guard().is_current_confirmation(flow, data.replace("btx:17:", "ai:")))
        flow["draft_id"] = 18
        self.assertFalse(self.guard().is_current_confirmation(flow, data))


if __name__ == "__main__":
    unittest.main()
