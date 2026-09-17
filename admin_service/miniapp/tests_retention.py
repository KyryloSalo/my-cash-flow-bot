from __future__ import annotations

from datetime import timedelta
from io import StringIO
from unittest.mock import ANY, patch

from django.core.management import CommandError, call_command
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from miniapp.models import AcquisitionSession, FunnelEvent
from miniapp.retention import purge_funnel_telemetry


class PurgeFunnelTelemetryCommandTests(SimpleTestCase):
    def test_command_requires_one_explicit_cutoff(self) -> None:
        with self.assertRaisesMessage(CommandError, "Provide exactly one"):
            call_command("purge_funnel_telemetry", stdout=StringIO())

    @patch(
        "miniapp.retention.purge_funnel_telemetry",
        return_value={"events": 7, "sessions": 2, "batches": 0},
    )
    def test_command_is_dry_run_by_default(self, purge) -> None:
        output = StringIO()

        call_command(
            "purge_funnel_telemetry",
            before="2026-06-01T00:00:00Z",
            stdout=output,
        )

        purge.assert_called_once_with(cutoff=ANY, apply=False, batch_size=1000)
        self.assertIn("DRY-RUN", output.getvalue())


class FunnelTelemetryRetentionTests(TestCase):
    def test_dry_run_and_apply_remove_only_rows_before_cutoff(self) -> None:
        now = timezone.now()
        cutoff = now - timedelta(days=30)
        old_session = AcquisitionSession.objects.create(expires_at=now + timedelta(days=1))
        old_event = FunnelEvent.objects.create(
            acquisition_session=old_session,
            event_name="landing_view",
            idempotency_key="old-event",
            payload_hash="0" * 64,
            source=FunnelEvent.Source.CLIENT,
        )
        fresh_session = AcquisitionSession.objects.create(expires_at=now + timedelta(days=1))
        fresh_event = FunnelEvent.objects.create(
            acquisition_session=fresh_session,
            event_name="landing_view",
            idempotency_key="fresh-event",
            payload_hash="1" * 64,
            source=FunnelEvent.Source.CLIENT,
        )
        AcquisitionSession.objects.filter(pk=old_session.pk).update(
            created_at=now - timedelta(days=60)
        )
        FunnelEvent.objects.filter(pk=old_event.pk).update(
            recorded_at=now - timedelta(days=60)
        )

        preview = purge_funnel_telemetry(cutoff=cutoff, apply=False, batch_size=1)

        self.assertEqual(preview, {"events": 1, "sessions": 1, "batches": 0})
        self.assertTrue(FunnelEvent.objects.filter(pk=old_event.pk).exists())
        applied = purge_funnel_telemetry(cutoff=cutoff, apply=True, batch_size=1)
        self.assertEqual(applied, {"events": 1, "sessions": 1, "batches": 2})
        self.assertFalse(FunnelEvent.objects.filter(pk=old_event.pk).exists())
        self.assertFalse(AcquisitionSession.objects.filter(pk=old_session.pk).exists())
        self.assertTrue(FunnelEvent.objects.filter(pk=fresh_event.pk).exists())
        self.assertTrue(AcquisitionSession.objects.filter(pk=fresh_session.pk).exists())
