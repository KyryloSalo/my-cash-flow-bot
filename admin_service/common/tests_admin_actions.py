from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.template.response import TemplateResponse
from django.test import RequestFactory, SimpleTestCase

from accounts.admin import AccountAdmin
from accounts.models import Account
from categories.admin import CategoryAdmin
from categories.models import Category
from common.admin_site import admin_site
from feedback.admin import FeedbackItemAdmin, create_support_case_from_feedback
from feedback.models import FeedbackItem
from polls.admin import PollCampaignAdmin
from polls.models import PollCampaign
from subscriptions.admin import confirm_manually, mark_refunded, reject_payment


class AuthorizedRequestFactory(RequestFactory):
    """Confirmation tests run as a real authenticated, authorized admin."""
    def request(self, **request):
        from django.contrib.auth import get_user_model
        result = super().request(**request)
        result.user = get_user_model()(pk=1, is_active=True, is_staff=True, is_superuser=True)
        return result


class ConfirmedAdminActionsTests(SimpleTestCase):
    def setUp(self):
        self.factory = AuthorizedRequestFactory()

    def _payment_queryset(self):
        queryset = MagicMock()
        queryset.count.return_value = 2
        queryset.select_related.return_value.order_by.return_value.__getitem__.return_value = []
        return queryset

    def test_payment_actions_render_confirmation_before_write(self):
        for action, action_name in (
            (confirm_manually, "confirm_manually"),
            (reject_payment, "reject_payment"),
            (mark_refunded, "mark_refunded"),
        ):
            with self.subTest(action_name=action_name):
                request = self.factory.post(
                    "/admin/subscriptions/payment/",
                    {"action": action_name, "_selected_action": ["11", "12"]},
                )
                queryset = self._payment_queryset()
                with patch.object(admin_site, "each_context", return_value={}):
                    response = action(None, request, queryset)

                self.assertIsInstance(response, TemplateResponse)
                self.assertEqual(response.template_name, "admin/payment_action_confirmation.html")
                queryset.update.assert_not_called()

    def test_operational_bulk_actions_render_confirmation_before_write(self):
        account_admin = AccountAdmin(Account, admin_site)
        category_admin = CategoryAdmin(Category, admin_site)
        feedback_admin = FeedbackItemAdmin(FeedbackItem, admin_site)
        actions = (
            ("make_default", lambda request, queryset: account_admin.make_default(request, queryset)),
            ("deactivate_accounts", lambda request, queryset: account_admin.deactivate_accounts(request, queryset)),
            ("deactivate_categories", lambda request, queryset: category_admin.deactivate_categories(request, queryset)),
            (
                "create_support_case_from_feedback",
                lambda request, queryset: create_support_case_from_feedback(feedback_admin, request, queryset),
            ),
        )

        for action_name, action in actions:
            with self.subTest(action_name=action_name):
                request = self.factory.post(
                    "/admin/action/",
                    {"action": action_name, "_selected_action": ["11", "12"]},
                )
                queryset = MagicMock()
                queryset.count.return_value = 2
                queryset.__getitem__.return_value = []
                with patch.object(admin_site, "each_context", return_value={}):
                    response = action(request, queryset)

                self.assertIsInstance(response, TemplateResponse)
                self.assertEqual(response.template_name, "admin/bulk_action_confirmation.html")
                queryset.update.assert_not_called()

    def test_poll_send_get_only_renders_confirmation(self):
        campaign = SimpleNamespace(pk=7, id=7, title="NPS серпень", status="draft")
        request = self.factory.get("/admin/polls/pollcampaign/7/send/")
        model_admin = PollCampaignAdmin(PollCampaign, admin_site)

        with (
            patch("polls.admin.get_object_or_404", return_value=campaign),
            patch.object(admin_site, "each_context", return_value={}),
            patch("polls.admin.send_poll_campaign_task.delay") as delay,
        ):
            response = model_admin.send_view(request, "7")

        self.assertIsInstance(response, TemplateResponse)
        self.assertEqual(response.template_name, "admin/object_action_form.html")
        delay.assert_not_called()

    def test_poll_send_post_requires_checked_confirmation(self):
        campaign = SimpleNamespace(pk=7, id=7, title="NPS серпень", status="draft")
        request = self.factory.post("/admin/polls/pollcampaign/7/send/", {"confirm": "on"})
        model_admin = PollCampaignAdmin(PollCampaign, admin_site)

        with (
            patch("polls.admin.get_object_or_404", return_value=campaign),
            patch("polls.admin.send_poll_campaign_task.delay") as delay,
            patch("polls.admin.create_audit_log"),
            patch("polls.admin.messages.success"),
        ):
            response = model_admin.send_view(request, "7")

        self.assertEqual(response.status_code, 302)
        delay.assert_called_once_with(7)

    def test_poll_complete_get_only_renders_confirmation(self):
        campaign = SimpleNamespace(pk=7, id=7, title="NPS серпень", status="active")
        request = self.factory.get("/admin/polls/pollcampaign/7/complete/")
        model_admin = PollCampaignAdmin(PollCampaign, admin_site)

        with (
            patch("polls.admin.get_object_or_404", return_value=campaign),
            patch.object(admin_site, "each_context", return_value={}),
        ):
            response = model_admin.complete_view(request, "7")

        self.assertIsInstance(response, TemplateResponse)
        self.assertEqual(response.template_name, "admin/object_action_form.html")
        self.assertEqual(campaign.status, "active")

    def test_poll_complete_post_updates_only_after_confirmation(self):
        campaign = SimpleNamespace(
            pk=7,
            id=7,
            title="NPS серпень",
            status="active",
            completed_at=None,
            save=MagicMock(),
        )
        request = self.factory.post("/admin/polls/pollcampaign/7/complete/", {"confirm": "on"})
        model_admin = PollCampaignAdmin(PollCampaign, admin_site)

        with (
            patch("polls.admin.get_object_or_404", return_value=campaign),
            patch("polls.admin.create_audit_log"),
            patch("polls.admin.messages.success"),
        ):
            response = model_admin.complete_view(request, "7")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(campaign.status, PollCampaign.Status.COMPLETED)
        self.assertIsNotNone(campaign.completed_at)
        campaign.save.assert_called_once_with(update_fields=["status", "completed_at", "updated_at"])
