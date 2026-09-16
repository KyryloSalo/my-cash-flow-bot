from django.db import migrations


def seed_segments(apps, schema_editor):
    Segment = apps.get_model("broadcasts", "Segment")
    defaults = [
        ("All Users", "all_users", "system", "All users", True),
        ("Active 7d", "active_7d", "system", "Seen within 7 days", True),
        ("Active 30d", "active_30d", "system", "Seen within 30 days", True),
        ("Inactive 14d", "inactive_14d", "system", "Not seen for 14 days", True),
        ("Inactive 30d", "inactive_30d", "system", "Not seen for 30 days", True),
        ("Onboarding Not Completed", "onboarding_not_completed", "system", "Users who did not finish onboarding", True),
        ("Trial Users", "trial_users", "system", "Users on trial", True),
        ("Trial Expiring 1d", "trial_expiring_1d", "system", "Trial expires within 1 day", True),
        ("Trial Expiring 3d", "trial_expiring_3d", "system", "Trial expires within 3 days", True),
        ("Paid Users", "paid_users", "system", "Paid/manual/lifetime users", True),
        ("Subscription Expiring Today", "subscription_expiring_today", "system", "Subscriptions ending today", True),
        ("Subscription Expiring 3d", "subscription_expiring_3d", "system", "Subscriptions ending within 3 days", True),
        ("Subscription Expiring 7d", "subscription_expiring_7d", "system", "Subscriptions ending within 7 days", True),
        ("Expired Users", "expired_users", "system", "Users with expired subscription", True),
        ("Users by Tag", "users_by_tag", "tag", "Segment resolved through target tag", True),
        ("Users by Source", "users_by_source", "source", "Segment resolved through source field", True),
    ]
    for name, slug, segment_type, description, is_system in defaults:
        Segment.objects.get_or_create(
            slug=slug,
            defaults={
                "name": name,
                "type": segment_type,
                "description": description,
                "is_active": True,
                "is_system": is_system,
            },
        )


def noop(apps, schema_editor):
    return None


class Migration(migrations.Migration):
    dependencies = [
        ("broadcasts", "0002_segment_adminmessagelog_buttons_payload_and_more"),
    ]

    operations = [
        migrations.RunPython(seed_segments, noop),
    ]
