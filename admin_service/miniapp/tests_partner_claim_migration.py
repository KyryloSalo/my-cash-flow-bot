import importlib

from django.test import TestCase
from django.utils import timezone

mark_legacy_claims = importlib.import_module(
    "miniapp.migrations.0019_partnertrialclaim_activated_at"
).mark_legacy_claims
from miniapp.models import PartnerTrialClaim
from subscriptions.models import BillingProfile, Subscription
from users.models import TelegramUser


class PartnerClaimMigrationTests(TestCase):
    def test_only_bound_trials_and_existing_entitlements_are_marked_used(self):
        cardless_user = TelegramUser.objects.create(tg_user_id=991901, first_name="Cardless")
        bound_user = TelegramUser.objects.create(tg_user_id=991902, first_name="Bound")
        paid_user = TelegramUser.objects.create(tg_user_id=991903, first_name="Paid")

        claims = {
            "cardless": PartnerTrialClaim.objects.create(
                user=cardless_user, offer_id="partner_link:1", code="legacy", name="Legacy", trial_days=31
            ),
            "bound": PartnerTrialClaim.objects.create(
                user=bound_user, offer_id="partner_link:2", code="bound", name="Bound", trial_days=17
            ),
            "paid": PartnerTrialClaim.objects.create(
                user=paid_user, offer_id="partner_link:3", code="paid", name="Paid", trial_days=9
            ),
        }
        Subscription.objects.create(user=cardless_user, status=Subscription.Status.TRIAL, expires_at=timezone.now())
        Subscription.objects.create(user=bound_user, status=Subscription.Status.TRIAL, expires_at=timezone.now())
        paid_subscription = Subscription.objects.create(
            user=paid_user, status=Subscription.Status.MANUAL, expires_at=timezone.now()
        )
        BillingProfile.objects.create(
            user=bound_user, wallet_id="migration-bound", card_token="tok_bound", status=BillingProfile.Status.ACTIVE
        )

        apps = {
            ("miniapp", "PartnerTrialClaim"): PartnerTrialClaim,
            ("subscriptions", "Subscription"): Subscription,
            ("subscriptions", "BillingProfile"): BillingProfile,
        }
        mark_legacy_claims(type("Apps", (), {"get_model": lambda _, app, model: apps[(app, model)]})(), None)

        self.assertIsNone(PartnerTrialClaim.objects.get(pk=claims["cardless"].pk).activated_at)
        self.assertIsNotNone(PartnerTrialClaim.objects.get(pk=claims["bound"].pk).activated_at)
        self.assertIsNotNone(PartnerTrialClaim.objects.get(pk=claims["paid"].pk).activated_at)
        paid_subscription.refresh_from_db()
        self.assertEqual(paid_subscription.status, Subscription.Status.MANUAL)
