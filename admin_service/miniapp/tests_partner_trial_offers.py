from datetime import timedelta

from django.test import SimpleTestCase, TestCase
from django.test import RequestFactory
from django.utils import timezone

from common.admin_site import admin_site
from miniapp.models import AcquisitionSession, PartnerLink, PartnerTrialClaim
from miniapp.partner_offers import claim_partner_offer
from subscriptions.models import Subscription
from users.models import TelegramUser, UserAdminState


class PartnerTrialConfigurationTests(SimpleTestCase):
    def test_owner_can_set_free_days_alongside_a_manual_hash(self):
        model_admin = admin_site._registry[PartnerLink]
        self.assertIn("trial_days", model_admin.fields)
        self.assertIn("code", model_admin.fields)
        field = PartnerLink._meta.get_field("trial_days")
        self.assertTrue(field.blank)
        self.assertTrue(field.null)


class PartnerTrialClaimTests(TestCase):
    def test_claim_snapshots_exact_arbitrary_days_once(self):
        link = PartnerLink.objects.create(name="Creator", code="creator51", utm_source="creator", trial_days=51)
        session = AcquisitionSession.objects.create(expires_at=timezone.now() + timedelta(days=365), first_partner_link=link, last_partner_link=link)
        user = TelegramUser.objects.create(tg_user_id=990001, first_name="Test")
        claim = claim_partner_offer(user=user, acquisition=session)
        self.assertEqual(claim.trial_days, 51)
        link.trial_days = 7
        link.save(update_fields=["trial_days"])
        self.assertEqual(claim_partner_offer(user=user, acquisition=session).trial_days, 51)

    def test_blank_days_does_not_claim(self):
        link = PartnerLink.objects.create(name="Organic", code="organic1", utm_source="organic")
        session = AcquisitionSession.objects.create(expires_at=timezone.now() + timedelta(days=365), first_partner_link=link, last_partner_link=link)
        user = TelegramUser.objects.create(tg_user_id=990002, first_name="Test")
        self.assertIsNone(claim_partner_offer(user=user, acquisition=session))
        self.assertFalse(PartnerTrialClaim.objects.filter(user=user).exists())

    def test_claim_grants_exact_trial_and_updates_access(self):
        link = PartnerLink.objects.create(name="Creator", code="creator51", utm_source="creator", trial_days=51)
        session = AcquisitionSession.objects.create(expires_at=timezone.now() + timedelta(days=365), first_partner_link=link, last_partner_link=link)
        user = TelegramUser.objects.create(tg_user_id=990003, first_name="Test")
        claim_partner_offer(user=user, acquisition=session)
        subscription = Subscription.objects.get(user=user)
        self.assertEqual(subscription.trial_days, 51)
        self.assertEqual(subscription.status, Subscription.Status.TRIAL)
        self.assertEqual(subscription.source, Subscription.Source.PROMO)
        self.assertFalse(subscription.auto_renew)
        self.assertAlmostEqual((subscription.expires_at - subscription.started_at).total_seconds(), 51 * 86400, delta=2)
        state = UserAdminState.objects.get(telegram_user_id=user.tg_user_id)
        self.assertEqual(state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)
        self.assertEqual(state.subscription_status, UserAdminState.SubscriptionStatus.TRIAL)

    def test_reentry_does_not_extend_or_duplicate_grant(self):
        link = PartnerLink.objects.create(name="Creator", code="creator17", utm_source="creator", trial_days=17)
        session = AcquisitionSession.objects.create(expires_at=timezone.now() + timedelta(days=365), first_partner_link=link, last_partner_link=link)
        user = TelegramUser.objects.create(tg_user_id=990004, first_name="Test")
        first = claim_partner_offer(user=user, acquisition=session)
        expires_at = first.claimed_at
        claim_partner_offer(user=user, acquisition=session)
        self.assertEqual(Subscription.objects.filter(user=user).count(), 1)
        self.assertEqual(PartnerTrialClaim.objects.get(user=user).claimed_at, expires_at)

    def test_existing_paid_user_is_not_downgraded_or_granted(self):
        link = PartnerLink.objects.create(name="Creator", code="creator9", utm_source="creator", trial_days=9)
        session = AcquisitionSession.objects.create(expires_at=timezone.now() + timedelta(days=365), first_partner_link=link, last_partner_link=link)
        user = TelegramUser.objects.create(tg_user_id=990005, first_name="Test")
        Subscription.objects.create(user=user, status=Subscription.Status.ACTIVE, source=Subscription.Source.PAYMENT, expires_at=timezone.now() + timedelta(days=90))
        UserAdminState.objects.create(telegram_user_id=user.tg_user_id, subscription_status=UserAdminState.SubscriptionStatus.PAID, access_scope=UserAdminState.AccessScope.PERSONAL_FULL)
        self.assertIsNone(claim_partner_offer(user=user, acquisition=session))
        self.assertEqual(Subscription.objects.filter(user=user).count(), 1)
        self.assertFalse(PartnerTrialClaim.objects.filter(user=user).exists())
