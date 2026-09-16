from __future__ import annotations

import json
from datetime import UTC, datetime, time, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.test import Client, SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from subscriptions.models import Payment, TrialRecoveryCampaign, TrialRecoveryDelivery, TrialRecoveryRecipient
from subscriptions.recovery_reporting import build_recovery_campaign_dashboard
from subscriptions.trial_recovery import (
    REGISTERED_WITHOUT_CARD_AUDIENCE,
    _message_payload,
    audience_preview,
    coerce_to_send_window,
    dispatch_trial_recovery,
    expand_campaign_audience,
    launch_campaign,
    record_trial_offer_event,
    safe_zone,
)
from users.models import TelegramUser, UserAdminState


class TrialRecoveryWindowTests(SimpleTestCase):
    def test_invalid_and_empty_timezones_fall_back_to_kyiv(self):
        self.assertEqual(safe_zone("")[1], "Europe/Kyiv")
        self.assertEqual(safe_zone("Definitely/Invalid")[1], "Europe/Kyiv")

    def test_night_due_time_moves_to_ten_local(self):
        due = datetime(2026, 8, 25, 22, 30, tzinfo=UTC)
        scheduled = coerce_to_send_window(
            due,
            timezone_name="Europe/Kyiv",
            window_start=time(10, 0),
            window_end=time(19, 0),
        )
        local = scheduled.astimezone(ZoneInfo("Europe/Kyiv"))
        self.assertEqual(local.date().isoformat(), "2026-08-26")
        self.assertEqual(local.time().replace(tzinfo=None), time(10, 0))


@override_settings(TELEGRAM_BOT_TOKEN="test-token", ADMIN_TEST_TELEGRAM_IDS=[])
class TrialRecoveryFlowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.admin_user = get_user_model().objects.create_superuser(
            "trial-recovery-admin",
            "trial-recovery@example.com",
            "pass12345",
        )
        self.campaign = TrialRecoveryCampaign.objects.create(
            name="Recovery test",
            status=TrialRecoveryCampaign.Status.DRAFT,
            fallback_timezone="Europe/Kyiv",
            send_window_start=time(10, 0),
            send_window_end=time(19, 0),
        )

    def make_user(self, tg_user_id: int, *, timezone_name: str = "Europe/Kyiv", access_scope: str = "paywall"):
        user = TelegramUser.objects.create(
            tg_user_id=tg_user_id,
            first_name="Recovery",
            username=f"recovery_{tg_user_id}",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        UserAdminState.objects.create(
            telegram_user=user,
            status=UserAdminState.Status.ACTIVE,
            subscription_status=UserAdminState.SubscriptionStatus.NONE,
            access_scope=access_scope,
            pending_start_payload="course",
            timezone=timezone_name,
            can_receive_messages=True,
        )
        return user

    @patch("subscriptions.trial_recovery.send_telegram_message")
    def test_draft_campaign_never_dispatches(self, send_message):
        user = self.make_user(91001)
        TrialRecoveryRecipient.objects.create(
            campaign=self.campaign,
            user=user,
            timezone="Europe/Kyiv",
            next_send_at=timezone.now() - timedelta(minutes=1),
        )

        result = dispatch_trial_recovery()

        self.assertEqual(result["selected"], 0)
        send_message.assert_not_called()

    def test_preview_targets_all_registered_without_payment_or_card(self):
        self.make_user(91002)
        self.make_user(91003, access_scope=UserAdminState.AccessScope.FAMILY_FULL)
        incomplete = self.make_user(91006)
        TelegramUser.objects.filter(pk=incomplete.pk).update(onboarding_completed=False)
        missing_state = self.make_user(91019)
        UserAdminState.objects.filter(telegram_user=missing_state).delete()
        blocked = self.make_user(91007)
        UserAdminState.objects.filter(telegram_user=blocked).update(can_receive_messages=False)
        paid = self.make_user(91008)
        Payment.objects.create(
            user=paid,
            amount="1.00",
            currency="UAH",
            kind=Payment.Kind.BIND,
            status=Payment.Status.PAID,
            provider_payment_id="paid-preview",
        )

        preview = audience_preview()

        self.assertEqual(preview["audience_mode"], REGISTERED_WITHOUT_CARD_AUDIENCE)
        self.assertEqual(preview["total_registered_users"], 6)
        self.assertEqual(preview["eligible"], 4)
        self.assertEqual(preview["excluded"]["cannot_receive_messages"], 1)
        self.assertEqual(preview["excluded"]["has_successful_payment"], 1)

    def test_launch_repairs_missing_admin_state_for_registered_user(self):
        user = self.make_user(91005)
        UserAdminState.objects.filter(telegram_user=user).delete()

        launch_campaign(self.campaign, launch_at=timezone.now(), admin_user=self.admin_user)

        self.assertTrue(UserAdminState.objects.filter(telegram_user=user).exists())
        self.assertTrue(TrialRecoveryRecipient.objects.filter(campaign=self.campaign, user=user).exists())

    def test_admin_launch_requires_explicit_confirmation(self):
        self.make_user(91004)
        self.client.force_login(self.admin_user)
        url = reverse("admin:subscriptions_trialrecoverycampaign_launch", args=[self.campaign.pk])

        preview = self.client.get(url)
        unconfirmed = self.client.post(url, {})
        self.campaign.refresh_from_db()

        self.assertEqual(preview.status_code, 200)
        self.assertContains(preview, "Запустити кампанію")
        self.assertEqual(unconfirmed.status_code, 200)
        self.assertEqual(self.campaign.status, TrialRecoveryCampaign.Status.DRAFT)

        confirmed = self.client.post(url, {"confirm": "on"})
        self.campaign.refresh_from_db()

        self.assertEqual(confirmed.status_code, 302)
        self.assertEqual(self.campaign.status, TrialRecoveryCampaign.Status.RUNNING)
        self.assertEqual(TrialRecoveryRecipient.objects.filter(campaign=self.campaign).count(), 1)

    def test_admin_campaign_page_renders_plain_language_dashboard(self):
        self.client.force_login(self.admin_user)

        response = self.client.get(
            reverse("admin:subscriptions_trialrecoverycampaign_change", args=[self.campaign.pk])
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Сьогодні")
        self.assertContains(response, "Відновлені користувачі")
        self.assertContains(response, "Що означає «відновлений користувач»")
        self.assertContains(response, "Ще буде надіслано")
        self.assertContains(response, "Останні доставки")
        self.assertNotContains(response, '"sent_messages"')

    def test_reporting_separates_recovery_attribution_and_local_today(self):
        now_local = datetime(2026, 8, 27, 14, 0, tzinfo=ZoneInfo("Europe/Kyiv"))
        now = now_local.astimezone(UTC)
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = now - timedelta(days=1)
        self.campaign.audience_snapshot = {"eligible": 4, "total_offer_users": 9, "excluded": {"has_other_access": 5}}
        self.campaign.save(update_fields=["status", "launch_at", "audience_snapshot", "updated_at"])

        recovered_user = self.make_user(91020)
        recovered = TrialRecoveryRecipient.objects.create(
            campaign=self.campaign,
            user=recovered_user,
            status=TrialRecoveryRecipient.Status.CONVERTED,
            timezone="Europe/Kyiv",
            sent_count=1,
            first_sent_at=now - timedelta(hours=2),
            last_sent_at=now - timedelta(hours=2),
            converted_at=now - timedelta(hours=1),
        )
        TrialRecoveryDelivery.objects.create(
            recipient=recovered,
            step=1,
            status=TrialRecoveryDelivery.Status.SENT,
            sent_at=now - timedelta(hours=2),
        )

        early_user = self.make_user(91021)
        TrialRecoveryRecipient.objects.create(
            campaign=self.campaign,
            user=early_user,
            status=TrialRecoveryRecipient.Status.CONVERTED,
            timezone="Europe/Kyiv",
            converted_at=now - timedelta(hours=1),
        )

        waiting_user = self.make_user(91022)
        TrialRecoveryRecipient.objects.create(
            campaign=self.campaign,
            user=waiting_user,
            status=TrialRecoveryRecipient.Status.SCHEDULED,
            timezone="Europe/Kyiv",
            next_send_at=now + timedelta(hours=1),
        )

        failed_user = self.make_user(91023)
        failed = TrialRecoveryRecipient.objects.create(
            campaign=self.campaign,
            user=failed_user,
            status=TrialRecoveryRecipient.Status.DELIVERY_FAILED,
            timezone="Definitely/Invalid",
            last_error="Forbidden: bot was blocked by the user",
        )
        failed_delivery = TrialRecoveryDelivery.objects.create(
            recipient=failed,
            step=1,
            status=TrialRecoveryDelivery.Status.FAILED,
            error_message="Forbidden: bot was blocked by the user",
        )
        TrialRecoveryDelivery.objects.filter(pk=failed_delivery.pk).update(
            created_at=now - timedelta(minutes=30),
            updated_at=now - timedelta(minutes=30),
        )

        dashboard = build_recovery_campaign_dashboard(self.campaign, now=now)

        self.assertEqual(dashboard["totals"]["recovered"], 1)
        self.assertEqual(dashboard["totals"]["converted_total"], 2)
        self.assertEqual(dashboard["totals"]["converted_before_message"], 1)
        self.assertEqual(dashboard["today"]["reached"], 1)
        self.assertEqual(dashboard["today"]["remaining"], 1)
        self.assertEqual(dashboard["today"]["failed"], 1)
        self.assertEqual(dashboard["today"]["recovered"], 1)
        self.assertEqual(dashboard["today"]["progress"], "66,7%")
        self.assertAlmostEqual(dashboard["today"]["progress_css"], 66.67, places=2)
        self.assertEqual(dashboard["totals"]["blocked"], 1)

    def test_miniapp_event_enrolls_only_active_campaign_and_90_day_offer(self):
        user = self.make_user(91005)
        client = Client()
        session = client.session
        session["miniapp_tg_user_id"] = user.tg_user_id
        session["miniapp_auth_at"] = int(timezone.now().timestamp())
        session["miniapp_auth_mode"] = "telegram"
        session.save()
        url = reverse("miniapp:trial-recovery-event")

        draft_response = client.post(
            url,
            data=json.dumps({"event": "shown", "trial_days": 90}),
            content_type="application/json",
        )
        self.assertEqual(draft_response.status_code, 200)
        self.assertFalse(draft_response.json()["enrolled"])

        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = timezone.now()
        self.campaign.save(update_fields=["status", "launch_at", "updated_at"])
        wrong_offer = client.post(
            url,
            data=json.dumps({"event": "shown", "trial_days": 30}),
            content_type="application/json",
        )
        active_response = client.post(
            url,
            data=json.dumps({"event": "shown", "trial_days": 90}),
            content_type="application/json",
        )

        self.assertFalse(wrong_offer.json()["enrolled"])
        self.assertTrue(active_response.json()["enrolled"])
        self.assertTrue(TrialRecoveryRecipient.objects.filter(user=user, source=TrialRecoveryRecipient.Source.LIVE).exists())

    def test_expanded_campaign_accepts_non_90_day_event_and_incomplete_onboarding(self):
        user = self.make_user(91009)
        TelegramUser.objects.filter(pk=user.pk).update(onboarding_completed=False)
        user.refresh_from_db()
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = timezone.now()
        self.campaign.audience_snapshot = {
            "audience_mode": REGISTERED_WITHOUT_CARD_AUDIENCE,
            "audience_enabled_at": timezone.now().isoformat(),
        }
        self.campaign.save(update_fields=["status", "launch_at", "audience_snapshot", "updated_at"])

        recipient = record_trial_offer_event(user, "shown", trial_days=30)

        self.assertIsNotNone(recipient)
        self.assertFalse(recipient.user.onboarding_completed)

    def test_expansion_requires_explicit_call_and_includes_incomplete_and_family_users(self):
        existing_user = self.make_user(91010)
        TrialRecoveryRecipient.objects.create(campaign=self.campaign, user=existing_user, timezone="Europe/Kyiv")
        completed = self.make_user(91011)
        incomplete = self.make_user(91012)
        TelegramUser.objects.filter(pk=incomplete.pk).update(onboarding_completed=False)
        family = self.make_user(91013, access_scope=UserAdminState.AccessScope.FAMILY_FULL)
        paid = self.make_user(91014)
        Payment.objects.create(
            user=paid,
            amount="1.00",
            currency="UAH",
            kind=Payment.Kind.BIND,
            status=Payment.Status.PAID,
            provider_payment_id="paid-expansion",
        )
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = timezone.now() - timedelta(days=1)
        self.campaign.audience_snapshot = {"eligible": 1, "total_offer_users": 1, "excluded": {}}
        self.campaign.save(update_fields=["status", "launch_at", "audience_snapshot", "updated_at"])
        now = datetime(2026, 8, 27, 12, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(UTC)

        result = expand_campaign_audience(self.campaign, admin_user=self.admin_user, now=now)

        self.assertEqual(result["created_recipients"], 3)
        self.campaign.refresh_from_db()
        self.assertEqual(self.campaign.audience_snapshot["audience_mode"], REGISTERED_WITHOUT_CARD_AUDIENCE)
        self.assertEqual(TrialRecoveryRecipient.objects.filter(campaign=self.campaign).count(), 4)
        self.assertTrue(TrialRecoveryRecipient.objects.filter(user=completed).exists())
        self.assertTrue(TrialRecoveryRecipient.objects.filter(user=incomplete).exists())
        self.assertTrue(TrialRecoveryRecipient.objects.filter(user=family).exists())
        self.assertFalse(TrialRecoveryRecipient.objects.filter(user=paid).exists())

    def test_admin_expansion_requires_confirmation(self):
        existing_user = self.make_user(91015)
        TrialRecoveryRecipient.objects.create(campaign=self.campaign, user=existing_user, timezone="Europe/Kyiv")
        self.make_user(91016)
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = timezone.now() - timedelta(days=1)
        self.campaign.audience_snapshot = {"eligible": 1, "total_offer_users": 1, "excluded": {}}
        self.campaign.save(update_fields=["status", "launch_at", "audience_snapshot", "updated_at"])
        self.client.force_login(self.admin_user)
        url = reverse("admin:subscriptions_trialrecoverycampaign_expand", args=[self.campaign.pk])

        preview = self.client.get(url)
        unconfirmed = self.client.post(url, {})
        self.campaign.refresh_from_db()

        self.assertEqual(preview.status_code, 200)
        self.assertContains(preview, "Розширити аудиторію")
        self.assertEqual(unconfirmed.status_code, 200)
        self.assertNotIn("audience_mode", self.campaign.audience_snapshot)

        confirmed = self.client.post(url, {"confirm": "on"})
        self.campaign.refresh_from_db()

        self.assertEqual(confirmed.status_code, 302)
        self.assertEqual(self.campaign.audience_snapshot["audience_mode"], REGISTERED_WITHOUT_CARD_AUDIENCE)
        self.assertEqual(TrialRecoveryRecipient.objects.filter(campaign=self.campaign).count(), 2)

    def test_scheduler_enrolls_new_registration_six_hours_later_only_after_expansion(self):
        now_local = datetime(2026, 8, 27, 12, 0, tzinfo=ZoneInfo("Europe/Kyiv"))
        now = now_local.astimezone(UTC)
        enabled_at = now - timedelta(hours=1)
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = enabled_at
        self.campaign.audience_snapshot = {
            "audience_mode": REGISTERED_WITHOUT_CARD_AUDIENCE,
            "audience_enabled_at": enabled_at.isoformat(),
        }
        self.campaign.save(update_fields=["status", "launch_at", "audience_snapshot", "updated_at"])
        user = self.make_user(91017)
        registered_at = now - timedelta(minutes=30)
        TelegramUser.objects.filter(pk=user.pk).update(created_at=registered_at)

        result = dispatch_trial_recovery(now=now)

        recipient = TrialRecoveryRecipient.objects.get(user=user)
        self.assertEqual(result["enrolled"], 1)
        self.assertEqual(result["selected"], 0)
        self.assertEqual(recipient.source, TrialRecoveryRecipient.Source.LIVE)
        self.assertEqual(recipient.next_send_at, registered_at + timedelta(hours=6))

    def test_legacy_running_campaign_does_not_auto_expand_after_restart(self):
        now = datetime(2026, 8, 27, 12, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(UTC)
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = now - timedelta(days=1)
        self.campaign.audience_snapshot = {"eligible": 0, "total_offer_users": 0, "excluded": {}}
        self.campaign.save(update_fields=["status", "launch_at", "audience_snapshot", "updated_at"])
        new_user = self.make_user(91021)
        UserAdminState.objects.filter(telegram_user=new_user).update(pending_start_payload="")

        result = dispatch_trial_recovery(now=now)

        self.assertEqual(result.get("enrolled", 0), 0)
        self.assertFalse(TrialRecoveryRecipient.objects.filter(user=new_user).exists())

    def test_incomplete_onboarding_gets_specific_copy(self):
        user = self.make_user(91018)
        TelegramUser.objects.filter(pk=user.pk).update(onboarding_completed=False)
        user.refresh_from_db()
        recipient = TrialRecoveryRecipient.objects.create(campaign=self.campaign, user=user, timezone="Europe/Kyiv")

        text, _buttons = _message_payload(recipient, 1)

        self.assertIn("зареєструвалися", text)
        self.assertIn("не завершили налаштування", text)

    def test_manual_night_launch_spreads_backfill_only_inside_local_window(self):
        users = [self.make_user(91100 + index) for index in range(6)]
        local_launch = datetime(2026, 8, 25, 2, 0, tzinfo=ZoneInfo("Europe/Kyiv"))
        launch_at = local_launch.astimezone(UTC)

        with patch("subscriptions.trial_recovery.timezone.now", return_value=launch_at):
            result = launch_campaign(self.campaign, launch_at=launch_at, admin_user=self.admin_user)

        self.assertEqual(result["created_recipients"], len(users))
        slots = list(TrialRecoveryRecipient.objects.order_by("next_send_at").values_list("next_send_at", flat=True))
        self.assertEqual(len(slots), len(users))
        local_slots = [slot.astimezone(ZoneInfo("Europe/Kyiv")) for slot in slots]
        self.assertEqual({slot.date() for slot in local_slots}, {local_launch.date()})
        self.assertTrue(all(time(10, 0) <= slot.time().replace(tzinfo=None) < time(19, 0) for slot in local_slots))
        self.assertGreater(local_slots[-1], local_slots[0])

    def test_live_offer_waits_six_hours_then_moves_out_of_night(self):
        user = self.make_user(91201)
        offered_local = datetime(2026, 8, 25, 16, 30, tzinfo=ZoneInfo("Europe/Kyiv"))
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = offered_local.astimezone(UTC) - timedelta(hours=1)
        self.campaign.save(update_fields=["status", "launch_at", "updated_at"])

        with patch("subscriptions.trial_recovery.timezone.now", return_value=offered_local.astimezone(UTC)):
            recipient = record_trial_offer_event(user, "shown", trial_days=90)

        self.assertIsNotNone(recipient)
        scheduled_local = recipient.next_send_at.astimezone(ZoneInfo("Europe/Kyiv"))
        self.assertEqual(scheduled_local.date().isoformat(), "2026-08-26")
        self.assertEqual(scheduled_local.time().replace(tzinfo=None), time(10, 0))

    @patch("subscriptions.trial_recovery.send_telegram_message")
    def test_overdue_message_is_rescheduled_instead_of_flushed(self, send_message):
        user = self.make_user(91301)
        now = datetime(2026, 8, 25, 12, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(UTC)
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = now - timedelta(days=1)
        self.campaign.save(update_fields=["status", "launch_at", "updated_at"])
        recipient = TrialRecoveryRecipient.objects.create(
            campaign=self.campaign,
            user=user,
            timezone="Europe/Kyiv",
            next_send_at=now - timedelta(hours=2),
        )

        result = dispatch_trial_recovery(now=now)

        recipient.refresh_from_db()
        self.assertEqual(result["rescheduled"], 1)
        self.assertGreater(recipient.next_send_at, now)
        send_message.assert_not_called()

    @patch("subscriptions.trial_recovery.send_telegram_message")
    def test_repeated_scheduler_run_does_not_duplicate_delivery(self, send_message):
        send_message.return_value = {"result": {"message_id": 123}}
        user = self.make_user(91401)
        now = datetime(2026, 8, 25, 12, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(UTC)
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = now - timedelta(hours=1)
        self.campaign.save(update_fields=["status", "launch_at", "updated_at"])
        recipient = TrialRecoveryRecipient.objects.create(
            campaign=self.campaign,
            user=user,
            timezone="Europe/Kyiv",
            next_send_at=now,
        )

        dispatch_trial_recovery(now=now)
        dispatch_trial_recovery(now=now)

        recipient.refresh_from_db()
        self.assertEqual(recipient.sent_count, 1)
        self.assertEqual(TrialRecoveryDelivery.objects.filter(recipient=recipient, step=1).count(), 1)
        send_message.assert_called_once()

    @patch("subscriptions.trial_recovery.send_telegram_message")
    def test_two_recovery_messages_cannot_send_same_local_day(self, send_message):
        user = self.make_user(91501)
        now_local = datetime(2026, 8, 25, 14, 0, tzinfo=ZoneInfo("Europe/Kyiv"))
        now = now_local.astimezone(UTC)
        self.campaign.status = TrialRecoveryCampaign.Status.RUNNING
        self.campaign.launch_at = now - timedelta(days=1)
        self.campaign.save(update_fields=["status", "launch_at", "updated_at"])
        recipient = TrialRecoveryRecipient.objects.create(
            campaign=self.campaign,
            user=user,
            timezone="Europe/Kyiv",
            sent_count=1,
            first_sent_at=now - timedelta(hours=1),
            last_sent_at=now - timedelta(hours=1),
            last_sent_local_date=now_local.date(),
            next_send_at=now,
        )

        result = dispatch_trial_recovery(now=now)

        recipient.refresh_from_db()
        next_local = recipient.next_send_at.astimezone(ZoneInfo("Europe/Kyiv"))
        self.assertEqual(result["rescheduled"], 1)
        self.assertEqual(next_local.date(), now_local.date() + timedelta(days=1))
        send_message.assert_not_called()
