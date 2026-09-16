from django.db import migrations


def seed_bot_settings(apps, schema_editor):
    BotSetting = apps.get_model("bot_settings", "BotSetting")
    defaults = [
        (
            "reset_onboarding_notice_text",
            "Онбординг скинуто. Тепер напишіть /start у Telegram, щоб пройти його заново.",
            "User-facing text after admin resets onboarding.",
            "string",
        ),
        (
            "poll_low_rating_threshold",
            "3",
            "Threshold for low poll ratings that trigger follow-up flows.",
            "int",
        ),
        (
            "payment_link_trial_30",
            "",
            "Payment link for users on the 30-day free scenario.",
            "string",
        ),
        (
            "payment_link_trial_90",
            "",
            "Payment link for users on the 90-day free scenario.",
            "string",
        ),
    ]
    for key, value, description, value_type in defaults:
        BotSetting.objects.get_or_create(
            key=key,
            defaults={
                "value": value,
                "description": description,
                "value_type": value_type,
            },
        )


def noop(apps, schema_editor):
    return None


class Migration(migrations.Migration):
    dependencies = [
        ("bot_settings", "0002_seed_defaults"),
    ]

    operations = [
        migrations.RunPython(seed_bot_settings, noop),
    ]
