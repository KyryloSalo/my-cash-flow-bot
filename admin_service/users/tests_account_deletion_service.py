from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import patch
from uuid import uuid4

from django.db import connection, connections, transaction
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from users import services
from users.models import TelegramUser, UserOidcTokenUse
from users.services import delete_own_account


class SelfServiceDeletionAtomicityTests(TestCase):
    def test_receipt_failure_rolls_back_privacy_graph_deletion(self) -> None:
        user = TelegramUser.objects.create(
            tg_user_id=901101,
            first_name="Atomic",
            username="atomic_delete",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=3,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        UserOidcTokenUse.objects.create(
            token_hash="d" * 64,
            provider="telegram_oidc",
            subject="atomic-delete-subject",
            tg_user_id=user.tg_user_id,
            expires_at=timezone.now(),
        )

        with patch("users.services.create_audit_log", side_effect=RuntimeError("audit unavailable")):
            with self.assertRaisesRegex(RuntimeError, "audit unavailable"):
                delete_own_account(user_id=user.tg_user_id, request_id=str(uuid4()))

        self.assertTrue(TelegramUser.objects.filter(pk=user.tg_user_id).exists())
        self.assertEqual(
            UserOidcTokenUse.objects.get(token_hash="d" * 64).tg_user_id,
            user.tg_user_id,
        )


class SelfServiceDeletionConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def test_owned_family_lock_serializes_concurrent_member_join(self) -> None:
        owner = TelegramUser.objects.create(
            tg_user_id=901201,
            first_name="Owner",
            lang="uk",
            base_currency="UAH",
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        member = TelegramUser.objects.create(
            tg_user_id=901202,
            first_name="Member",
            lang="uk",
            base_currency="UAH",
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO families (name, owner_user_id, status, created_at, updated_at) "
                "VALUES (%s, %s, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id",
                ["Concurrent", owner.tg_user_id],
            )
            family_id = int(cursor.fetchone()[0])
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, 'owner', 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, owner.tg_user_id, owner.tg_user_id],
            )

        validation_entered = Event()
        release_deletion = Event()
        writer_locked = Event()
        original_lock = services._lock_owned_family_ids

        def pause_after_lock(**kwargs):
            family_ids = original_lock(**kwargs)
            validation_entered.set()
            self.assertTrue(release_deletion.wait(5), "deletion validation was not released")
            return family_ids

        def delete_owner():
            try:
                with patch("users.services._lock_owned_family_ids", side_effect=pause_after_lock):
                    return delete_own_account(user_id=owner.tg_user_id, request_id=str(uuid4()))
            finally:
                connections["default"].close()

        def join_after_family_lock():
            try:
                with transaction.atomic():
                    with connections["default"].cursor() as cursor:
                        cursor.execute("SELECT id FROM families WHERE id = %s FOR UPDATE", [family_id])
                        row = cursor.fetchone()
                        writer_locked.set()
                        if row is not None:
                            cursor.execute(
                                "INSERT INTO family_members "
                                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                                "VALUES (%s, %s, 'member', 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                                [family_id, member.tg_user_id, owner.tg_user_id],
                            )
            finally:
                connections["default"].close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            delete_future = pool.submit(delete_owner)
            self.assertTrue(validation_entered.wait(5), "deletion never reached locked validation")
            join_future = pool.submit(join_after_family_lock)
            self.assertFalse(writer_locked.wait(0.5), "concurrent writer bypassed the family row lock")
            release_deletion.set()
            result = delete_future.result(timeout=10)
            join_future.result(timeout=10)

        self.assertEqual(result["status"], "deleted")
        self.assertFalse(TelegramUser.objects.filter(pk=owner.pk).exists())
        self.assertTrue(TelegramUser.objects.filter(pk=member.pk).exists())
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM family_members WHERE user_id = %s", [member.tg_user_id])
            self.assertEqual(int(cursor.fetchone()[0]), 0)
