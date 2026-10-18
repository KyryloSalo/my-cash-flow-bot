from datetime import timedelta

from django.test import SimpleTestCase, TestCase
from django.test import RequestFactory
from django.utils import timezone

from common.admin_site import admin_site
from miniapp.models import AcquisitionSession, PartnerLink, PartnerTrialClaim
from miniapp.partner_offers import claim_partner_offer, public_partner_offer
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
    def test_claim_grants_exact_7889_days_from_real_partner_offer(self):
        link = PartnerLink.objects.create(name="Launch partner", code="launch7889", utm_source="launch", trial_days=7889)
        session = AcquisitionSession.objects.create(
            expires_at=timezone.now() + timedelta(days=365),
            first_partner_link=link,
            last_partner_link=link,
        )
        user = TelegramUser.objects.create(tg_user_id=990000, first_name="Test")

        claim = claim_partner_offer(user=user, acquisition=session)

        self.assertFalse(Subscription.objects.filter(user=user).exists())
        self.assertEqual(claim.trial_days, 7889)
        self.assertEqual(public_partner_offer(claim=claim)["trial_days"], 7889)
        self.assertFalse(UserAdminState.objects.filter(telegram_user_id=user.tg_user_id, access_scope=UserAdminState.AccessScope.PERSONAL_FULL).exists())

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
        self.assertFalse(Subscription.objects.filter(user=user).exists())
        self.assertFalse(UserAdminState.objects.filter(telegram_user_id=user.tg_user_id, access_scope=UserAdminState.AccessScope.PERSONAL_FULL).exists())

    def test_reentry_does_not_extend_or_duplicate_grant(self):
        link = PartnerLink.objects.create(name="Creator", code="creator17", utm_source="creator", trial_days=17)
        session = AcquisitionSession.objects.create(expires_at=timezone.now() + timedelta(days=365), first_partner_link=link, last_partner_link=link)
        user = TelegramUser.objects.create(tg_user_id=990004, first_name="Test")
        first = claim_partner_offer(user=user, acquisition=session)
        expires_at = first.claimed_at
        claim_partner_offer(user=user, acquisition=session)
        self.assertEqual(Subscription.objects.filter(user=user).count(), 0)
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
