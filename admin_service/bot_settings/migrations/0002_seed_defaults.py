from django.db import migrations


def seed_bot_settings(apps, schema_editor):
    BotSetting = apps.get_model("bot_settings", "BotSetting")
    defaults = [
        ("base_currency", "UAH", "Default base currency", "string"),
        ("trial_duration_days", "7", "Default trial duration in days", "int"),
        ("registration_enabled", "true", "Allow new users to register", "bool"),
        ("maintenance_mode", "false", "Enable maintenance mode", "bool"),
        ("maintenance_message", "Бот тимчасово на технічних роботах. Спробуй пізніше.", "Maintenance mode message", "string"),
        ("support_admin_telegram_id", "", "Primary support admin telegram id", "string"),
        ("support_contact_text", "Опиши, що сталося, одним повідомленням.", "Support prompt for bot users", "string"),
        ("payment_link", "", "Default payment link", "string"),
        ("default_subscription_expiring_text", "Підписка скоро завершиться. Продовж доступ, щоб не втратити функції.", "Default expiring subscription text", "string"),
        ("default_subscription_expired_text", "Підписка завершилась. Продовж доступ, щоб знову користуватися всіма функціями.", "Default expired subscription text", "string"),
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
        ("bot_settings", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(seed_bot_settings, noop),
    ]
