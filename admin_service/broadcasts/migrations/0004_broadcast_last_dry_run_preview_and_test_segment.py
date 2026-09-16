from django.db import migrations, models


def seed_test_users_segment(apps, schema_editor):
    Segment = apps.get_model("broadcasts", "Segment")
    Segment.objects.get_or_create(
        slug="test_users",
        defaults={
            "name": "Test Users",
            "type": "system",
            "description": "Users marked as test users or allowlisted for QA",
            "is_active": True,
            "is_system": True,
        },
    )


def noop(apps, schema_editor):
    return None


class Migration(migrations.Migration):
    dependencies = [
        ("broadcasts", "0003_seed_segments"),
    ]

    operations = [
        migrations.AddField(
            model_name="broadcast",
            name="last_dry_run_preview",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.RunPython(seed_test_users_segment, noop),
    ]
