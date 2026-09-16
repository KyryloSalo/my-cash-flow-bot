from datetime import UTC, datetime
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from common.readiness import _billing_lag


class BillingReadinessTests(SimpleTestCase):
    @patch("django.utils.timezone.now")
    @patch("subscriptions.models.Subscription.objects.filter")
    @patch("subscriptions.models.Payment.objects.filter")
    def test_due_subscription_without_chargeable_profile_does_not_block_readiness(
        self,
        payment_filter,
        subscription_filter,
        now,
    ):
        now.return_value = datetime(2030, 1, 15, 12, tzinfo=UTC)
        payment_filter.return_value.exists.return_value = False

        def subscription_queryset(**filters):
            queryset = Mock()
            queryset.exists.return_value = (
                "user__billing_profile__card_token__gt" not in filters
            )
            return queryset

        subscription_filter.side_effect = subscription_queryset

        self.assertTrue(_billing_lag())
        filters = subscription_filter.call_args.kwargs
        self.assertEqual(filters["user__billing_profile__provider"], "monobank")
        self.assertIs(filters["user__billing_profile__auto_renew_enabled"], True)
        self.assertEqual(filters["user__billing_profile__card_token__gt"], "")
