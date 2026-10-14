from django.db import migrations


SYNTHETIC_CANARY_TELEGRAM_ID = 777777
SYNTHETIC_CANARY_SOURCE = "production_canary_seed"


def activate_synthetic_canary_identity(apps, schema_editor):
    UserAdminState = apps.get_model("users", "UserAdminState")
    updated = UserAdminState.objects.filter(
        telegram_user_id=SYNTHETIC_CANARY_TELEGRAM_ID,
        source=SYNTHETIC_CANARY_SOURCE,
    ).update(
        status="active",
        is_blocked=False,
        can_receive_messages=False,
        is_test_user=True,
        is_production_canary=True,
    )
    if updated != 1:
        raise RuntimeError(
            "Cannot activate Telegram ID 777777 as the production canary: "
            "the reserved seed state is missing or ambiguous."
        )


def deactivate_synthetic_canary_identity(apps, schema_editor):
    UserAdminState = apps.get_model("users", "UserAdminState")
    updated = UserAdminState.objects.filter(
        telegram_user_id=SYNTHETIC_CANARY_TELEGRAM_ID,
        source=SYNTHETIC_CANARY_SOURCE,
    ).update(is_production_canary=False)
    if updated != 1:
        raise RuntimeError(
            "Cannot deactivate Telegram ID 777777 during rollback: "
            "the reserved seed state is missing or ambiguous."
        )


class Migration(migrations.Migration):
    dependencies = [
        ("users", "0015_seed_synthetic_canary_identity"),
    ]

    operations = [
        migrations.RunPython(
            activate_synthetic_canary_identity,
            deactivate_synthetic_canary_identity,
        ),
    ]
