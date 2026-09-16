"""ADMIN-008: invalid targets and changed confirmed audiences fail closed."""
from types import SimpleNamespace

from django.core.exceptions import ValidationError

from broadcasts.models import Broadcast, Segment
from broadcasts.targets import users_for_target, users_for_segment_slug
from broadcasts.tasks import send_broadcast_task
from broadcasts.tests_communications_remediation import CommunicationsDatabaseTests, Session, queued_poll_args
from broadcasts.wizard import remember_broadcast_test, broadcast_test_matches, recipient_group_from_broadcast, apply_recipients_form
from polls.models import PollCampaign
from polls.tasks import send_poll_campaign_task
from users.models import Tag, UserTag


class AudienceFailClosedRegressionTests(CommunicationsDatabaseTests):
    def test_missing_segment_is_not_presented_as_all_in_wizard(self):
        self.assertEqual(recipient_group_from_broadcast(Broadcast(target_type="segment")), "")

    def test_invalid_wizard_group_cannot_become_all(self):
        broadcast = Broadcast()
        with self.assertRaises(ValueError):
            apply_recipients_form(broadcast, {"recipient_group": "invalid"})

    def test_missing_or_unknown_target_is_an_explicit_error(self):
        for target in ("tag", "segment", "push_topic", "invalid", ""):
            with self.subTest(target=target), self.assertRaises(ValueError):
                users_for_target(target_type=target)
        self.assertEqual(users_for_target(target_type="all").count(), 3)

    def test_unknown_or_incomplete_segment_is_an_explicit_error(self):
        for slug in ("invalid", "users_by_tag", "users_by_source"):
            with self.subTest(slug=slug), self.assertRaises(ValueError):
                users_for_segment_slug(slug)

    def test_model_rejects_missing_required_tag(self):
        broadcast = Broadcast(title="Synthetic", message_text="Stub", created_by=self.admin, target_type="tag")
        with self.assertRaises(ValidationError):
            broadcast.clean()

    def test_deleted_tag_after_broadcast_preview_stops_delivery(self):
        tag = Tag.objects.create(name="Synthetic", slug="synthetic")
        UserTag.objects.create(user=self.users[0], tag=tag)
        broadcast = self.broadcast(target_type="tag", target_tag=tag)
        tag.delete()
        send_broadcast_task(broadcast.pk)
        self.send.assert_not_called()
        broadcast.refresh_from_db()
        self.assertEqual(broadcast.status, Broadcast.Status.FAILED)
        self.assertTrue(broadcast.last_error)

    def test_same_count_audience_replacement_requires_new_broadcast_preview(self):
        tag = Tag.objects.create(name="Synthetic", slug="synthetic")
        link = UserTag.objects.create(user=self.users[0], tag=tag)
        broadcast = self.broadcast(target_type="tag", target_tag=tag)
        link.delete()
        UserTag.objects.create(user=self.users[1], tag=tag)
        send_broadcast_task(broadcast.pk)
        self.send.assert_not_called()
        broadcast.refresh_from_db()
        self.assertEqual(broadcast.status, Broadcast.Status.FAILED)

    def test_changed_message_requires_new_broadcast_preview(self):
        broadcast = self.broadcast()
        Broadcast.objects.filter(pk=broadcast.pk).update(message_text="Changed after approval")
        send_broadcast_task(broadcast.pk)
        self.send.assert_not_called()

    def test_queued_poll_rejects_a_deleted_tag(self):
        tag = Tag.objects.create(name="Synthetic", slug="synthetic")
        UserTag.objects.create(user=self.users[0], tag=tag)
        campaign = self.poll(target_type="tag", target_tag=tag)
        args, kwargs = queued_poll_args(campaign.pk)
        tag.delete()
        send_poll_campaign_task(*args, **kwargs)
        self.poll_send.assert_not_called()
        campaign.refresh_from_db()
        self.assertEqual(campaign.status, PollCampaign.Status.CANCELLED)

    def test_queued_poll_rejects_changed_config(self):
        campaign = self.poll()
        args, kwargs = queued_poll_args(campaign.pk)
        PollCampaign.objects.filter(pk=campaign.pk).update(question="Changed after confirmation")
        send_poll_campaign_task(*args, **kwargs)
        self.poll_send.assert_not_called()

    def test_poll_without_confirmed_snapshot_cannot_send(self):
        campaign = self.poll()
        send_poll_campaign_task(campaign.pk)
        self.poll_send.assert_not_called()

    def test_same_count_audience_change_invalidates_test_confirmation(self):
        tag = Tag.objects.create(name="Synthetic", slug="synthetic")
        link = UserTag.objects.create(user=self.users[0], tag=tag)
        broadcast = self.broadcast(target_type="tag", target_tag=tag)
        request = SimpleNamespace(user=self.admin, session=Session())
        remember_broadcast_test(request, broadcast)
        link.delete()
        UserTag.objects.create(user=self.users[1], tag=tag)
        self.assertFalse(broadcast_test_matches(request, broadcast))
