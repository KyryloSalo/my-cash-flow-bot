from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("admin_notifications", "0001_initial")]

    operations = [
        migrations.AlterField(
            model_name="adminnotificationlog", name="status",
            field=models.CharField(choices=[("queued", "Queued"), ("sent", "Sent"),
                ("failed", "Failed"), ("processing", "Processing"), ("dead", "Dead letter")],
                default="queued", max_length=16),
        ),
        migrations.AddField(model_name="adminnotificationlog", name="attempt_count",
                            field=models.PositiveSmallIntegerField(default=0)),
        migrations.AddField(model_name="adminnotificationlog", name="next_retry_at",
                            field=models.DateTimeField(blank=True, null=True)),
        migrations.AddField(model_name="adminnotificationlog", name="lease_token",
                            field=models.CharField(blank=True, default="", max_length=32)),
        migrations.AddField(model_name="adminnotificationlog", name="lease_expires_at",
                            field=models.DateTimeField(blank=True, null=True)),
        migrations.AddIndex(model_name="adminnotificationlog", index=models.Index(
            fields=["event_type", "status", "next_retry_at"], name="admin_photo_retry_idx")),
    ]
