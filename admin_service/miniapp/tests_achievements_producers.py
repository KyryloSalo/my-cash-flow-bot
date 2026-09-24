from datetime import UTC, datetime
from decimal import Decimal

from django.test import TestCase

from gamification.models import GamificationEventOutbox
from gamification.producers import enqueue_transaction_created


class TransactionEventProducerTests(TestCase):
    def test_enqueue_is_idempotent_and_normalizes_input_method(self):
        accepted_at = datetime(2026, 9, 1, 9, 30, tzinfo=UTC)

        first = enqueue_transaction_created(
            actor_user_id=1001,
            space_id=None,
            transaction_id=501,
            accepted_at=accepted_at,
            source="miniapp_manual",
            transaction_type="expense",
            amount=Decimal("25.50"),
            flow_kind="normal",
        )
        second = enqueue_transaction_created(
            actor_user_id=1001,
            space_id=None,
            transaction_id=501,
            accepted_at=accepted_at,
            source="miniapp_manual",
            transaction_type="expense",
            amount=Decimal("25.50"),
            flow_kind="normal",
        )

        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(GamificationEventOutbox.objects.count(), 1)
        self.assertEqual(first.input_method, "manual")
        self.assertEqual(first.payload["amount"], "25.50")

    def test_import_is_preserved_and_unknown_sources_are_not_relabelled_as_manual(self):
        accepted_at = datetime(2026, 9, 1, 9, 30, tzinfo=UTC)

        imported = enqueue_transaction_created(
            actor_user_id=1001,
            space_id=None,
            transaction_id=502,
            accepted_at=accepted_at,
            source="import",
            transaction_type="expense",
            amount=Decimal("10.00"),
            flow_kind="normal",
        )
        unknown = enqueue_transaction_created(
            actor_user_id=1001,
            space_id=None,
            transaction_id=503,
            accepted_at=accepted_at,
            source="untrusted-client-label",
            transaction_type="expense",
            amount=Decimal("10.00"),
            flow_kind="normal",
        )

        self.assertEqual(imported.input_method, "import")
        self.assertEqual(unknown.input_method, "")
