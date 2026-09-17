from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from subscriptions import billing


class PaidConsentHookTests(SimpleTestCase):
    @patch("subscriptions.billing.send_bind_activation_notification")
    @patch("subscriptions.trial_recovery.mark_trial_recovery_converted")
    @patch("subscriptions.billing.finalize_paid_consent")
    @patch("subscriptions.billing._apply_bind_subscription_state")
    @patch("subscriptions.billing._upsert_billing_profile")
    @patch("subscriptions.billing._extract_masked_pan", return_value="4444")
    @patch("subscriptions.billing._resolve_paid_save_card_payload", return_value=("card-token", {}))
    @patch("subscriptions.billing._payment_mandate_is_current", return_value=True)
    @patch("subscriptions.billing._payment_bind_trial_granted", return_value=True)
    @patch("subscriptions.billing._bind_mode", return_value="bind")
    @patch("subscriptions.billing._bind_context", return_value={"trial_days": 30})
    @patch("subscriptions.billing.Subscription.objects.filter")
    def test_bind_success_records_payment_after_subscription_state(
        self,
        subscription_filter,
        _bind_context,
        _bind_mode,
        _trial_granted,
        _mandate_current,
        _save_card_payload,
        _masked_pan,
        _upsert_profile,
        apply_subscription_state,
        finalize_consent,
        _mark_recovery,
        _notify,
    ) -> None:
        subscription_filter.return_value.exists.return_value = False
        first_renewal_at = datetime(2026, 10, 16, tzinfo=timezone.utc)
        subscription = SimpleNamespace(next_charge_at=first_renewal_at)
        apply_subscription_state.return_value = subscription
        payment = SimpleNamespace(
            pk=41,
            user_id=812345,
            user=SimpleNamespace(tg_user_id=812345),
            provider_modified_at=None,
            status="paid",
        )

        billing._handle_bind_success(payment, {}, apply_effect=True)

        finalize_consent.assert_called_once_with(
            payment=payment,
            first_renewal_at=first_renewal_at,
        )
