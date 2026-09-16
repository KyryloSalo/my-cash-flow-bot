"""Operational uncertainty labels and fail-closed poll model validation."""
from django.core.exceptions import ValidationError
from django.test import SimpleTestCase

from broadcasts.admin import BroadcastAdmin, MESSAGE_STATUS_LABELS, RECIPIENT_STATUS_LABELS
from broadcasts.models import Broadcast
from common.admin_site import admin_site
from polls.models import PollCampaign, PollRecipient


class DeliverySurfaceIntegrationTests(SimpleTestCase):
    def test_uncertain_delivery_is_human_readable_and_counter_is_read_only(self):
        for labels in (MESSAGE_STATUS_LABELS, RECIPIENT_STATUS_LABELS):
            self.assertIn("uncertain", labels)
            self.assertIn("невідом", labels["uncertain"])
        model_admin = BroadcastAdmin(Broadcast, admin_site)
        self.assertIn("uncertain_count", model_admin.readonly_fields)
        self.assertIn("uncertain_count", model_admin.list_display)
        self.assertIn("uncertain", dict(PollRecipient.Status.choices))

    def test_poll_model_rejects_missing_audience_references(self):
        for target_type in ("segment", "tag", "push_topic", "not_a_target"):
            with self.subTest(target_type=target_type):
                campaign = PollCampaign(target_type=target_type, title="Synthetic", question="Stub")
                with self.assertRaises(ValidationError):
                    campaign.clean()
        PollCampaign(target_type="all", title="Synthetic", question="Stub").clean()
