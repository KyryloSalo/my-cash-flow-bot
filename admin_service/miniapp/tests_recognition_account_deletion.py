import tempfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path
from threading import Event
from uuid import uuid4

from django.db import connection, connections, transaction
from django.test import TransactionTestCase, override_settings

from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
from miniapp.recognition_quality import capture_attempt
from miniapp.recognition_storage import _locked_config
from users.models import TelegramUser
from users.services import _SELF_SERVICE_DELETE_CAPABILITY, hard_delete_user


class RecognitionAccountDeletionTests(TransactionTestCase):
    def test_account_deletion_waits_for_export_lock_before_deleting_rows(self):
        if connection.vendor != 'postgresql':
            self.skipTest('Requires PostgreSQL advisory locks')
        user = TelegramUser.objects.create(tg_user_id=991004, first_name='Synthetic')
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[user.pk])
        capture = RecognitionCapture.objects.create(user_id=user.pk, source='text', raw_text='synthetic')
        lock_requested = Event()

        def delete_in_worker():
            def observe(execute, sql, params, many, context):
                if 'pg_advisory_xact_lock' in sql:
                    lock_requested.set()
                return execute(sql, params, many, context)

            try:
                with connections['default'].execute_wrapper(observe):
                    return hard_delete_user(
                        user.pk, admin_user=None, reason='self_service_account_deletion',
                        options={'_self_service_delete_capability': _SELF_SERVICE_DELETE_CAPABILITY,
                                 'self_service_request_id': str(uuid4())},
                    )
            finally:
                connections.close_all()

        with tempfile.TemporaryDirectory() as storage, override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=storage):
            with ThreadPoolExecutor(max_workers=1) as pool:
                with transaction.atomic():
                    _locked_config()
                    pending = pool.submit(delete_in_worker)
                    self.assertTrue(lock_requested.wait(timeout=15), 'Deletion did not request the storage lock')
                    self.assertTrue(RecognitionCapture.objects.filter(pk=capture.pk).exists())
                    self.assertTrue(TelegramUser.objects.filter(pk=user.pk).exists())
                pending.result(timeout=15)
        self.assertFalse(RecognitionCapture.objects.filter(pk=capture.pk).exists())

    def test_inflight_capture_waits_for_deletion_then_rejects_missing_account(self):
        if connection.vendor != 'postgresql':
            self.skipTest('Requires PostgreSQL row locks')
        user = TelegramUser.objects.create(tg_user_id=991003, first_name='Synthetic')
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[user.pk])
        query_started = Event()

        def capture_in_worker():
            def observe(execute, sql, params, many, context):
                if 'FOR UPDATE' in sql and TelegramUser._meta.db_table in sql:
                    query_started.set()
                return execute(sql, params, many, context)

            try:
                with connections['default'].execute_wrapper(observe):
                    return capture_attempt(user_id=991003, source='text', raw_text='concurrent synthetic input')
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                TelegramUser.objects.select_for_update().get(pk=user.pk)
                pending = pool.submit(capture_in_worker)
                self.assertTrue(query_started.wait(timeout=10), 'Capture did not attempt the account row lock')
                with self.assertRaises(TimeoutError):
                    pending.result(timeout=0.2)
                hard_delete_user(
                    user.pk, admin_user=None, reason='self_service_account_deletion',
                    options={'_self_service_delete_capability': _SELF_SERVICE_DELETE_CAPABILITY,
                             'self_service_request_id': str(uuid4())},
                )
            self.assertIsNone(pending.result(timeout=10))
        self.assertFalse(RecognitionCapture.objects.filter(user_id=991003).exists())

    def test_capture_rejects_deleted_account_even_if_cohort_contains_it(self):
        user = TelegramUser.objects.create(tg_user_id=991002, first_name="Synthetic")
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[user.pk])
        hard_delete_user(
            user.pk, admin_user=None, reason="self_service_account_deletion",
            options={"_self_service_delete_capability": _SELF_SERVICE_DELETE_CAPABILITY,
                     "self_service_request_id": str(uuid4())},
        )
        with tempfile.TemporaryDirectory() as storage, override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=storage):
            result = capture_attempt(user_id=991002, source='text', raw_text='late synthetic input')
            self.assertIsNone(result)
            self.assertFalse(RecognitionCapture.objects.filter(user_id=991002).exists())
            self.assertEqual(list(Path(storage).iterdir()), [])

    def test_hard_delete_removes_capture_rows_and_private_media_after_commit(self):
        user = TelegramUser.objects.create(tg_user_id=991001, first_name="Synthetic")
        with tempfile.TemporaryDirectory() as storage:
            root = Path(storage)
            media_key = "a" * 32 + ".bin"
            (root / media_key).write_bytes(b"private synthetic media")
            capture = RecognitionCapture.objects.create(
                attempt_id="synthetic-delete-1",
                user_id=user.tg_user_id,
                source="camera",
                raw_media=media_key,
                raw_media_size=22,
            )
            with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(root)):
                result = hard_delete_user(
                    user.tg_user_id,
                    admin_user=None,
                    reason="self_service_account_deletion",
                    options={
                        "_self_service_delete_capability": _SELF_SERVICE_DELETE_CAPABILITY,
                        "self_service_request_id": str(uuid4()),
                    },
                )

            self.assertEqual(result["cleanup"]["deleted_recognition_captures"], 1)
            self.assertFalse(RecognitionCapture.objects.filter(pk=capture.pk).exists())
            self.assertFalse((root / media_key).exists())
            self.assertFalse(TelegramUser.objects.filter(pk=user.pk).exists())
