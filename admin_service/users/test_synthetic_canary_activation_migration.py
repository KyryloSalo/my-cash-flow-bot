from __future__ import annotations

import importlib
from unittest import TestCase


class _Manager:
    def __init__(self, updated=1):
        self.updated = updated
        self.filter_kwargs = None
        self.update_kwargs = None

    def filter(self, **kwargs):
        self.filter_kwargs = kwargs
        return self

    def update(self, **kwargs):
        self.update_kwargs = kwargs
        return self.updated


class _StateModel:
    objects = _Manager()


class _Apps:
    @staticmethod
    def get_model(app_label, model_name):
        if (app_label, model_name) != ("users", "UserAdminState"):
            raise AssertionError((app_label, model_name))
        return _StateModel


class SyntheticCanaryActivationMigrationTests(TestCase):
    def setUp(self):
        _StateModel.objects = _Manager()
        self.migration = importlib.import_module(
            "users.migrations.0016_activate_synthetic_canary_identity"
        )

    def test_activates_only_the_reserved_seed_identity(self):
        self.migration.activate_synthetic_canary_identity(_Apps(), object())

        self.assertEqual(
            _StateModel.objects.filter_kwargs,
            {
                "telegram_user_id": 777777,
                "source": "production_canary_seed",
            },
        )
        self.assertEqual(
            _StateModel.objects.update_kwargs,
            {
                "status": "active",
                "is_blocked": False,
                "can_receive_messages": False,
                "is_test_user": True,
                "is_production_canary": True,
            },
        )

    def test_fails_closed_when_reserved_seed_state_is_missing(self):
        _StateModel.objects = _Manager(updated=0)

        with self.assertRaisesRegex(RuntimeError, "Telegram ID 777777"):
            self.migration.activate_synthetic_canary_identity(_Apps(), object())

    def test_fails_closed_when_reserved_seed_state_is_ambiguous(self):
        _StateModel.objects = _Manager(updated=2)

        with self.assertRaisesRegex(RuntimeError, "Telegram ID 777777"):
            self.migration.activate_synthetic_canary_identity(_Apps(), object())

    def test_rollback_deactivates_only_the_reserved_seed_identity(self):
        self.migration.deactivate_synthetic_canary_identity(_Apps(), object())

        self.assertEqual(
            _StateModel.objects.filter_kwargs,
            {
                "telegram_user_id": 777777,
                "source": "production_canary_seed",
            },
        )
        self.assertEqual(
            _StateModel.objects.update_kwargs,
            {"is_production_canary": False},
        )
