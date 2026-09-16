from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("audit_log", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="adminauditlog",
            name="mode",
            field=models.CharField(blank=True, default="", max_length=64),
        ),
        migrations.AddField(
            model_name="adminauditlog",
            name="reason",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="adminauditlog",
            name="target_user_id",
            field=models.BigIntegerField(blank=True, null=True),
        ),
    ]
