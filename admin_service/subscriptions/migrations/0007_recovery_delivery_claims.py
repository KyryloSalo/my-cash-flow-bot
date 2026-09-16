from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("subscriptions", "0006_trial_recovery")]

    operations = [
        migrations.AddField(
            model_name="trialrecoveryrecipient",
            name="personal_contact_declined_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="trialrecoverydelivery",
            name="attempt_count",
            field=models.PositiveSmallIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="trialrecoverydelivery",
            name="claimed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="trialrecoverydelivery",
            name="next_retry_at",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
        migrations.AlterField(
            model_name="trialrecoverydelivery",
            name="status",
            field=models.CharField(
                choices=[
                    ("claimed", "Claimed"),
                    ("retry", "Retry"),
                    ("unknown", "Outcome unknown"),
                    ("sent", "Sent"),
                    ("failed", "Failed"),
                    ("skipped", "Skipped"),
                ],
                default="claimed",
                max_length=16,
            ),
        ),
    ]
