from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("broadcasts", "0005_adminmessagelog_operator_nullable")]

    operations = [
        migrations.AddField(
            model_name="broadcast",
            name="uncertain_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AlterField(
            model_name="broadcastrecipient",
            name="status",
            field=models.CharField(
                max_length=32, default="pending",
                choices=[
                    ("pending", "Pending"),
                    ("uncertain", "In flight / outcome unknown (no automatic retry)"),
                    ("sent", "Sent"), ("failed", "Failed"),
                    ("blocked", "Blocked"), ("skipped", "Skipped"),
                ],
            ),
        ),
        migrations.AlterField(
            model_name="adminmessagelog",
            name="status",
            field=models.CharField(
                max_length=32, default="queued",
                choices=[
                    ("queued", "Queued"),
                    ("uncertain", "In flight / outcome unknown (no automatic retry)"),
                    ("sent", "Sent"), ("failed", "Failed"),
                    ("blocked", "Blocked"), ("skipped", "Skipped"),
                ],
            ),
        ),
    ]
