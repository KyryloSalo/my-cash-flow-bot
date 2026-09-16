"""Align the unmanaged ORM mirror; runtime SQL already requires these values."""
from django.db import migrations, models
from django.utils import timezone


class Migration(migrations.Migration):
    dependencies = [("users", "0006_adminnote_operator_nullable")]
    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.AlterField(
                    model_name="telegramuser", name="created_at",
                    field=models.DateTimeField(default=timezone.now),
                ),
                migrations.AlterField(
                    model_name="telegramuser", name="last_seen_at",
                    field=models.DateTimeField(default=timezone.now),
                ),
            ],
        ),
    ]
