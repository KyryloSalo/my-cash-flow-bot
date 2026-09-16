from django.db import migrations


def seed_push_topics(apps, schema_editor):
    PushTopic = apps.get_model("users", "PushTopic")
    defaults = [
        ("System Alerts", "system_alerts", "System-critical notifications", True),
        ("Payment Reminders", "payment_reminders", "Payment and renewal reminders", False),
        ("Product Updates", "product_updates", "Product release notes and updates", False),
        ("Marketing", "marketing", "Marketing messages", False),
        ("Beta Features", "beta_features", "Beta announcements", False),
        ("Finance Tips", "finance_tips", "Finance/productivity tips", False),
        ("Partner Ads", "partner_ads", "Partner promotions", False),
    ]
    for name, slug, description, is_system in defaults:
        PushTopic.objects.get_or_create(
            slug=slug,
            defaults={
                "name": name,
                "description": description,
                "is_system": is_system,
                "is_active": True,
            },
        )


def noop(apps, schema_editor):
    return None


class Migration(migrations.Migration):
    dependencies = [
        ("users", "0002_pushtopic_tag_useradminstate_admin_comment_and_more"),
    ]

    operations = [
        migrations.RunPython(seed_push_topics, noop),
    ]
