"""A pair of real PostgreSQL admin scheduling contenders queues once."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connections
from django.test import TransactionTestCase

from broadcasts.admin import BroadcastAdmin
from broadcasts.models import Broadcast
from common.admin_site import admin_site


class AdminSchedulingConcurrencyTests(TransactionTestCase):
    def test_two_stale_confirmations_have_one_database_claim_winner(self):
        self.assertEqual(connections["default"].vendor, "postgresql")
        actor = get_user_model().objects.create(username="synthetic-scheduler", is_staff=True, is_superuser=True)
        campaign = Broadcast.objects.create(title="Synthetic", message_text="Stub", created_by=actor)
        barrier = Barrier(2)
        pids = []
        handler = BroadcastAdmin(Broadcast, admin_site)

        def contender():
            try:
                snapshot = Broadcast.objects.get(pk=campaign.pk)
                with connections["default"].cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    pids.append(cursor.fetchone()[0])
                barrier.wait(timeout=10)
                return handler._schedule_broadcast(SimpleNamespace(user=actor), snapshot)
            finally:
                connections.close_all()

        with patch("broadcasts.admin.messages.warning"), patch("broadcasts.admin.send_broadcast_task.delay") as queue:
            with ThreadPoolExecutor(max_workers=2) as pool:
                pending = [pool.submit(contender) for _ in range(2)]
                results = [item.result(timeout=20) for item in pending]
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(len(set(pids)), 2)
        self.assertEqual(queue.call_count, 1)
        campaign.refresh_from_db()
        self.assertEqual(campaign.status, "scheduled")
        print("ADMIN_PG_ENQUEUE_RACE", {"independent_connections": len(set(pids)), "queued": queue.call_count})
