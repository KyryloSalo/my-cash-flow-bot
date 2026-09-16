"""Run unchanged durable regression assertions with authenticated actor fixtures.

Batch1 fixtures predate the scope lock and hand-built batches without identity.
Only fixture setup is extended here; no runtime guard or assertion is mocked.
Use mini_confirm_postgres_runner.py, never a configured application database.
"""
from miniapp import auth, views
from miniapp import tests_mini_confirmation_integrity as integrity
from miniapp import tests_mini_confirmation_postgres as postgres
from users.models import TelegramUser
from django.utils import timezone


class AuthenticatedFixture:
    def setUp(self):
        super().setUp()
        TelegramUser.objects.get_or_create(
            tg_user_id=self.user.tg_user_id,
            defaults={"lang": "en", "base_currency": "UAH", "created_at": timezone.now()},
        )

    def request(self, payload, session):
        session[auth.SESSION_USER_ID_KEY] = self.user.tg_user_id
        batch = session.get(views.AI_IMAGE_BATCH_SESSION_KEY)
        if batch is not None:
            batch.setdefault("identity_scope", views._identity_scope_binding(self.user))
        return super().request(payload, session)


class IntegrityWithActor(AuthenticatedFixture, integrity.ConfirmationIntegrityTests):
    pass


class PostgresWithActor(AuthenticatedFixture, postgres.ConfirmationPostgresTests):
    pass