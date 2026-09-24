from datetime import UTC, datetime
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from common.readiness import _billing_lag, readiness_status


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

    def test_billing_backlog_does_not_block_runtime_readiness(self):
        billing_probe = Mock(return_value=False)
        always_ready = Mock(return_value=True)

        with patch.multiple(
            "common.readiness",
            _database=always_ready,
            _redis=always_ready,
            _worker=always_ready,
            _beat=always_ready,
            _queue_lag=always_ready,
            _billing_lag=billing_probe,
        ):
            result = readiness_status()

        self.assertTrue(result["ready"])
        self.assertNotIn("billing_lag", result["checks"])
        billing_probe.assert_not_called()
