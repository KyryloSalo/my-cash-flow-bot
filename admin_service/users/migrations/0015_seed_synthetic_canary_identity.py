from django.db import migrations


SYNTHETIC_CANARY_TELEGRAM_ID = 777777
SYNTHETIC_CANARY_SOURCE = "production_canary_seed"

SAFE_ADMIN_STATE = {
    "status": "active",
    "subscription_status": "none",
    "access_scope": "paywall",
    "source": SYNTHETIC_CANARY_SOURCE,
    "is_blocked": False,
    "can_receive_messages": False,
    "is_test_user": True,
    "test_user_notes": "Reserved synthetic browser-only canary identity.",
    "is_production_canary": False,
}


def _normalize_seed_state(state):
    for field, value in SAFE_ADMIN_STATE.items():
        setattr(state, field, value)
    state.save(update_fields=list(SAFE_ADMIN_STATE))


def seed_synthetic_canary_identity(apps, schema_editor):
    with schema_editor.connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO users (
                tg_user_id,
                first_name,
                lang,
                base_currency,
                onboarding_completed,
                onboarding_version,
                created_at,
                last_seen_at
            )
            VALUES (%s, %s, %s, %s, false, 0, NOW(), NOW())
            ON CONFLICT (tg_user_id) DO NOTHING
            RETURNING tg_user_id
            """,
            [SYNTHETIC_CANARY_TELEGRAM_ID, "Production Canary", "uk", "UAH"],
        )
        inserted_user = cursor.fetchone() is not None

    UserAdminState = apps.get_model("users", "UserAdminState")
    if not inserted_user:
        existing_state = UserAdminState.objects.filter(
            telegram_user_id=SYNTHETIC_CANARY_TELEGRAM_ID
        ).first()
        if existing_state is None or existing_state.source != SYNTHETIC_CANARY_SOURCE:
            raise RuntimeError(
                "Cannot reserve Telegram ID 777777: the ID already exists and is not "
                "owned by the production canary seed."
            )
        _normalize_seed_state(existing_state)
        return

    state, created = UserAdminState.objects.get_or_create(
        telegram_user_id=SYNTHETIC_CANARY_TELEGRAM_ID,
        defaults=SAFE_ADMIN_STATE,
    )
    if not created:
        if state.source != SYNTHETIC_CANARY_SOURCE:
            raise RuntimeError(
                "Cannot reserve Telegram ID 777777: its admin state is not owned by "
                "the production canary seed."
            )
        _normalize_seed_state(state)


class Migration(migrations.Migration):
    dependencies = [
        ("users", "0014_alter_useradminstate_options"),
    ]

    operations = [
        migrations.RunPython(seed_synthetic_canary_identity, migrations.RunPython.noop),
    ]
