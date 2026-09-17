from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from subscriptions.billing import revoke_billing_for_account_deletion
from subscriptions.models import BillingProfile, Subscription
from subscriptions.monobank import MonobankAPIError
from users.models import TelegramUser


class AccountDeletionBillingTests(TestCase):
    def setUp(self) -> None:
        self.user = TelegramUser.objects.create(
            tg_user_id=902001,
            first_name="Synthetic",
            username="synthetic_billing_delete",
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.profile = BillingProfile.objects.create(
            user=self.user,
            provider="monobank",
            wallet_id="delete-test-wallet",
            card_token="synthetic-card-token",
            masked_pan="4444 **** **** 1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        self.subscription = Subscription.objects.create(
            user=self.user,
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            auto_renew=True,
            next_charge_at=timezone.now() + timedelta(days=7),
        )

    @patch(
        "subscriptions.billing.delete_wallet_card",
        side_effect=MonobankAPIError("synthetic provider outage"),
    )
    def test_provider_failure_preserves_account_billing_state(self, delete_wallet_card) -> None:
        with self.assertRaises(MonobankAPIError):
            revoke_billing_for_account_deletion(user_id=self.user.tg_user_id)

        self.profile.refresh_from_db()
        self.subscription.refresh_from_db()
        self.assertEqual(self.profile.card_token, "synthetic-card-token")
        self.assertTrue(self.profile.auto_renew_enabled)
        self.assertTrue(self.subscription.auto_renew)
        self.assertIsNotNone(self.subscription.next_charge_at)
        delete_wallet_card.assert_called_once_with("synthetic-card-token")
