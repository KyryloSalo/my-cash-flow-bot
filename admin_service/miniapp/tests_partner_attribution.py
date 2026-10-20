from unittest.mock import patch

from django.test import RequestFactory, TestCase
from django.utils import timezone

from miniapp.funnel import record_request_server_event
from miniapp.models import (
    AcquisitionSession,
    DurableRegistrationAttribution,
    PartnerLink,
)
from users.models import TelegramUser


class DurableRegistrationAttributionTests(TestCase):
    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=101, created_at=timezone.now())
        self.link = PartnerLink.objects.create(name="Partner", code="partner", utm_source="partner")
        self.factory = RequestFactory()

    def request(self, session_data=None):
        request = self.factory.get("/")
        from django.contrib.sessions.middleware import SessionMiddleware
        SessionMiddleware(lambda request: None).process_request(request)
        request.session.update(session_data or {})
        request.session.save()
        return request

    def test_registration_snapshot_survives_telemetry_failure(self):
        session = AcquisitionSession.objects.create(
            first_partner_link=self.link,
            expires_at=timezone.now() + timezone.timedelta(days=1),
        )
        request = self.request({"funnel_acquisition_session_id": str(session.pk)})
        with patch("miniapp.funnel.record_server_event", side_effect=RuntimeError("telemetry")):
            with self.assertRaises(RuntimeError):
                record_request_server_event(
                    request, user=self.user, event_name="registration_completed", idempotency_key="registration:101"
                )
        snapshot = DurableRegistrationAttribution.objects.get(user=self.user)
        self.assertEqual(snapshot.partner_link_id, self.link.pk)
        self.assertEqual(snapshot.registered_at, self.user.created_at)

    def test_no_acquisition_creates_organic_snapshot_and_returns_none(self):
        result = record_request_server_event(
            self.request(), user=self.user, event_name="registration_completed", idempotency_key="registration:101"
        )
        self.assertIsNone(result)
        snapshot = DurableRegistrationAttribution.objects.get(user=self.user)
        self.assertIsNone(snapshot.partner_link_id)
        self.assertEqual(snapshot.registered_at, self.user.created_at)

    def test_registration_rolls_back_if_snapshot_cannot_be_persisted(self):
        from types import SimpleNamespace
        from miniapp.views import _bootstrap_with_partner

        def bootstrap(*args, **kwargs):
            user = TelegramUser.objects.create(tg_user_id=90909)
            return SimpleNamespace(created=True, user=user)

        with patch("miniapp.views.bootstrap_telegram_identity", side_effect=bootstrap):
            with patch.object(DurableRegistrationAttribution.objects, "get_or_create", side_effect=RuntimeError("snapshot write failed")):
                with self.assertRaisesMessage(RuntimeError, "snapshot write failed"):
                    _bootstrap_with_partner(self.request(), object(), source="test")
        self.assertFalse(TelegramUser.objects.filter(tg_user_id=90909).exists())

    def test_backfill_uses_first_server_registration_and_does_not_overwrite(self):
        import importlib
        from django.apps import apps
        from django.db import connection
        from miniapp.funnel import record_server_event

        session = AcquisitionSession.objects.create(user=self.user, first_partner_link=self.link, expires_at=timezone.now() + timezone.timedelta(days=1))
        record_server_event(acquisition=session, user=self.user, event_name="registration_completed", idempotency_key="backfill:101")
        migration = importlib.import_module("miniapp.migrations.0022_backfill_durable_registration_attribution")
        with connection.schema_editor(atomic=False) as editor:
            migration.backfill(apps, editor)
            migration.backfill(apps, editor)
        snapshot = DurableRegistrationAttribution.objects.get(user=self.user)
        self.assertEqual(snapshot.partner_link_id, self.link.pk)
        self.assertEqual(DurableRegistrationAttribution.objects.filter(user=self.user).count(), 1)

    def test_later_partner_click_does_not_replace_organic_snapshot(self):
        DurableRegistrationAttribution.objects.create(user=self.user, registered_at=self.user.created_at)
        session = AcquisitionSession.objects.create(
            first_partner_link=self.link,
            expires_at=timezone.now() + timezone.timedelta(days=1),
        )
        request = self.request({"funnel_acquisition_session_id": str(session.pk)})
        with patch("miniapp.funnel.record_server_event", return_value=(None, True)):
            record_request_server_event(
                request, user=self.user, event_name="registration_completed", idempotency_key="registration:101"
            )
        self.assertIsNone(DurableRegistrationAttribution.objects.get(user=self.user).partner_link_id)
