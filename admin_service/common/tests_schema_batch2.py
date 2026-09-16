"""Migration-backed PostgreSQL regressions for OPS-002 / OPS-003."""
from importlib import import_module
from unittest import skipUnless
from uuid import uuid4

from django.apps import apps
from django.db import connection, transaction, IntegrityError
from django.db.migrations.loader import MigrationLoader
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from accounts.models import Account, AccountAdminState
from feedback.models import FeedbackItem
from support.models import SupportCase
from users.models import TelegramUser, UserAdminState, Tag


@skipUnless(connection.vendor == "postgresql", "Requires real PostgreSQL runtime schema")
class SchemaRelationshipsTests(TestCase):
    def test_clean_migrations_supply_runtime_and_admin_relationships(self):
        self.assertEqual(connection.vendor, "postgresql")
        now = timezone.now()
        user = TelegramUser.objects.create(tg_user_id=987000001, created_at=now, last_seen_at=now)
        state = UserAdminState.objects.create(telegram_user=user)
        # Use the bot's raw SQL seam as well as the Django mirrors.
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO accounts(tg_user_id, label, currency) VALUES (%s, 'schema fixture', 'UAH') RETURNING id", [user.pk])
            account_id = cursor.fetchone()[0]
        account = Account.objects.get(pk=account_id)
        account_state = AccountAdminState.objects.create(account=account)
        support = SupportCase.objects.create(user=user, subject="synthetic")
        feedback = FeedbackItem.objects.create(user=user, support_case=support, text="synthetic")
        tag = Tag.objects.create(name="Schema fixture", slug="schema-fixture")
        feedback.tags.add(tag)
        feedback.tags.add(tag)
        self.assertEqual(list(feedback.tags.values_list("pk", flat=True)), [tag.pk])
        feedback.tags.remove(tag)
        self.assertFalse(feedback.tags.exists())
        self.assertEqual(state.telegram_user.pk, user.pk)
        self.assertEqual(account_state.account.tg_user_id, user.pk)
        self.assertEqual(feedback.support_case.user_id, user.pk)
        with self.assertRaises(IntegrityError), transaction.atomic():
            UserAdminState.objects.create(telegram_user_id=987000099)
            connection.check_constraints()
        with self.assertRaises(IntegrityError), transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO accounts(tg_user_id, label, currency) VALUES (987000099, 'orphan', 'UAH')")

    def test_every_runtime_mirror_column_exists(self):
        for model in apps.get_models():
            if model._meta.managed:
                continue
            with self.subTest(model=model._meta.label), connection.cursor() as cursor:
                columns = {item.name for item in connection.introspection.get_table_description(cursor, model._meta.db_table)}
                self.assertTrue({field.column for field in model._meta.local_fields} <= columns)

    def test_managed_tables_retain_bot_insert_defaults(self):
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO users(tg_user_id) VALUES (987000002)")
            cursor.execute("INSERT INTO bot_settings(key) VALUES ('schema-fixture-defaults') RETURNING value, value_type, updated_at")
            value, value_type, updated_at = cursor.fetchone()
            self.assertEqual((value, value_type), ("", "string"))
            self.assertIsNotNone(updated_at)
            cursor.execute("INSERT INTO support_cases(telegram_user_id) VALUES (987000002) RETURNING id")
            case_id = cursor.fetchone()[0]
            cursor.execute("INSERT INTO support_messages(case_id, sender_type, text) VALUES (%s, 'user', 'synthetic')", [case_id])
            cursor.execute("INSERT INTO feedback_items(telegram_user_id, text, support_case_id) VALUES (987000002, 'synthetic', %s) RETURNING status, created_at", [case_id])
            status, created_at = cursor.fetchone()
            self.assertEqual(status, "new")
            self.assertIsNotNone(created_at)


@skipUnless(connection.vendor == "postgresql", "Requires real PostgreSQL runtime schema")
class RuntimeFlushIsolationTests(TransactionTestCase):
    reset_sequences = True
    def test_01_runtime_rows_are_part_of_flush(self):
        from django.core.management.color import no_style
        from django.core.management.sql import sql_flush
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO users(tg_user_id) VALUES (987000003)")
            cursor.execute("INSERT INTO families(name, owner_user_id) VALUES ('synthetic', 987000003)")
        sql = " ".join(sql_flush(no_style(), connection, reset_sequences=False))
        self.assertIn('"families"', sql)
        self.assertIn('"users"', sql)

    def test_02_previous_runtime_rows_were_flushed(self):
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM users WHERE tg_user_id=987000003")
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT count(*) FROM families WHERE owner_user_id=987000003")
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("INSERT INTO users(tg_user_id) VALUES (987000003)")
            cursor.execute("INSERT INTO families(name, owner_user_id) VALUES ('synthetic reset', 987000003) RETURNING id")
            self.assertEqual(cursor.fetchone()[0], 1)


@skipUnless(connection.vendor == "postgresql", "Requires real PostgreSQL runtime schema")
class CoreBootstrapTests(TestCase):
    def test_core_only_bootstrap_is_idempotent_and_does_not_rewrite_finance(self):
        from common.runtime_schema import CORE_TABLES, ensure_runtime_schema
        schema = "schema_regression_" + uuid4().hex
        # This entire temporary schema is rolled back by TestCase.
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(f'SET LOCAL search_path TO "{schema}"')
        ensure_runtime_schema()
        self.assertEqual(set(connection.introspection.table_names()), set(CORE_TABLES))
        self.assertFalse(set(CORE_TABLES) & {model._meta.db_table for model in apps.get_models() if model._meta.managed})
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO users(tg_user_id) VALUES (987000004)")
            cursor.execute("INSERT INTO accounts(tg_user_id,label,currency,balance,account_type) VALUES (987000004,'preserve','UAH',-10,'other')")
            cursor.execute("SELECT oid FROM pg_class WHERE relnamespace = %s::regnamespace ORDER BY oid", [schema])
            before = cursor.fetchall()
        ensure_runtime_schema()
        with connection.cursor() as cursor:
            cursor.execute("SELECT oid FROM pg_class WHERE relnamespace = %s::regnamespace ORDER BY oid", [schema])
            self.assertEqual(cursor.fetchall(), before)
            cursor.execute("SELECT balance, account_type FROM accounts")
            self.assertEqual(cursor.fetchone(), (-10, "other"))
        # Restore public for TestCase's constraint checks; rollback removes the schema.
        with connection.cursor() as cursor:
            cursor.execute('SET LOCAL search_path TO public')


@skipUnless(connection.vendor == "postgresql", "Requires real PostgreSQL runtime schema")
class FeedbackRepairTests(TestCase):
    def repair(self):
        module = import_module("feedback.migrations.0002_repair_feedback_tags")
        historical_apps = MigrationLoader(connection).project_state([("feedback", "0001_initial")]).apps
        with connection.schema_editor() as editor:
            module.repair_feedback_tags(historical_apps, editor)

    def test_missing_legacy_through_table_gets_pk_uniqueness_and_foreign_keys(self):
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE feedback_items_tags")
        self.repair()
        with connection.cursor() as cursor:
            constraints = connection.introspection.get_constraints(cursor, "feedback_items_tags")
        self.assertTrue(any(item["primary_key"] and item["columns"] == ["id"] for item in constraints.values()))
        self.assertTrue(any(item["unique"] and set(item["columns"]) == {"feedbackitem_id", "tag_id"} for item in constraints.values()))
        self.assertEqual({tuple(item["foreign_key"]) for item in constraints.values() if item["foreign_key"]}, {("feedback_items", "id"), ("user_tags", "id")})
        item = FeedbackItem.objects.create(text="synthetic legacy feedback")
        tag = Tag.objects.create(name="legacy fixture", slug="legacy-fixture")
        item.tags.add(tag)
        self.repair()
        self.repair()
        self.assertEqual(list(item.tags.values_list("pk", flat=True)), [tag.pk])
        item.tags.remove(tag)
        self.assertFalse(item.tags.exists())
