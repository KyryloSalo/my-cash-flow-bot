from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("users", "0004_useradminstate_test_tools_and_debug"),
    ]

    operations = [
        migrations.AddField(
            model_name="useradminstate",
            name="access_scope",
            field=models.CharField(
                choices=[
                    ("personal_full", "Personal full"),
                    ("family_full", "Family full"),
                    ("debt_only", "Debt only"),
                    ("paywall", "Paywall"),
                ],
                default="paywall",
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="access_source",
            field=models.CharField(blank=True, default="", max_length=128),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="pending_start_payload",
            field=models.TextField(blank=True, default=""),
        ),
    ]
