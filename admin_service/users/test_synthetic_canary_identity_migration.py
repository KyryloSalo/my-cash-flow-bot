from __future__ import annotations

import importlib
from unittest import TestCase


class _Cursor:
    def __init__(self, fetchone_value=(777777,)):
        self.calls = []
        self.fetchone_value = fetchone_value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params):
        self.calls.append((sql, params))

    def fetchone(self):
        return self.fetchone_value


class _Connection:
    def __init__(self, fetchone_value=(777777,)):
        self.cursor_instance = _Cursor(fetchone_value)

    def cursor(self):
        return self.cursor_instance


class _Manager:
    def __init__(self, existing_state=None):
        self.calls = []
        self.existing_state = existing_state

    def filter(self, **kwargs):
        self.filter_kwargs = kwargs
        return self

    def first(self):
        return self.existing_state

    def get_or_create(self, **kwargs):
        self.calls.append(kwargs)
        return object(), True


class _StateModel:
    objects = _Manager()


class _Apps:
    @staticmethod
    def get_model(app_label, model_name):
        if (app_label, model_name) != ("users", "UserAdminState"):
            raise AssertionError((app_label, model_name))
        return _StateModel


class _SchemaEditor:
    connection = _Connection()


class _ExistingSeedState:
    source = "production_canary_seed"
    status = "blocked"
    is_test_user = False
    is_production_canary = True
    can_receive_messages = True

    def save(self, **kwargs):
        self.save_kwargs = kwargs


class SyntheticCanaryIdentityMigrationTests(TestCase):
    def setUp(self):
        _StateModel.objects = _Manager()
        _SchemaEditor.connection = _Connection()

    def test_seeds_reserved_identity_without_activating_canary(self):
        migration = importlib.import_module(
            "users.migrations.0015_seed_synthetic_canary_identity"
        )

        migration.seed_synthetic_canary_identity(_Apps(), _SchemaEditor())

        [(sql, params)] = _SchemaEditor.connection.cursor_instance.calls
        self.assertIn("INSERT INTO users", sql)
        self.assertIn("ON CONFLICT (tg_user_id) DO NOTHING", sql)
        self.assertIn("RETURNING tg_user_id", sql)
        self.assertEqual(params[0], 777777)
        self.assertEqual(params[1], "Production Canary")
        self.assertEqual(params[2], "uk")
        self.assertEqual(params[3], "UAH")

        [state_call] = _StateModel.objects.calls
        self.assertEqual(state_call["telegram_user_id"], 777777)
        self.assertEqual(
            state_call["defaults"],
            {
                "status": "active",
                "subscription_status": "none",
                "access_scope": "paywall",
                "source": "production_canary_seed",
                "is_blocked": False,
                "can_receive_messages": False,
                "is_test_user": True,
                "test_user_notes": "Reserved synthetic browser-only canary identity.",
                "is_production_canary": False,
            },
        )

    def test_fails_closed_when_reserved_id_already_belongs_to_another_user(self):
        conflicting_state = type(
            "ExistingState",
            (),
            {"source": "telegram", "can_receive_messages": True},
        )()
        _StateModel.objects = _Manager(existing_state=conflicting_state)
        _SchemaEditor.connection = _Connection(fetchone_value=None)
        migration = importlib.import_module(
            "users.migrations.0015_seed_synthetic_canary_identity"
        )

        with self.assertRaisesRegex(RuntimeError, "reserve Telegram ID 777777"):
            migration.seed_synthetic_canary_identity(_Apps(), _SchemaEditor())

        self.assertEqual(_StateModel.objects.calls, [])

    def test_rerun_normalizes_only_a_state_owned_by_the_seed(self):
        existing_state = _ExistingSeedState()
        _StateModel.objects = _Manager(existing_state=existing_state)
        _SchemaEditor.connection = _Connection(fetchone_value=None)
        migration = importlib.import_module(
            "users.migrations.0015_seed_synthetic_canary_identity"
        )

        migration.seed_synthetic_canary_identity(_Apps(), _SchemaEditor())

        self.assertEqual(existing_state.status, "active")
        self.assertTrue(existing_state.is_test_user)
        self.assertFalse(existing_state.is_production_canary)
        self.assertFalse(existing_state.can_receive_messages)
        self.assertEqual(
            set(existing_state.save_kwargs["update_fields"]),
            set(migration.SAFE_ADMIN_STATE),
        )
