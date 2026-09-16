from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("broadcasts", "0004_broadcast_last_dry_run_preview_and_test_segment"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AlterField(
            model_name="adminmessagelog",
            name="admin_user",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="admin_message_logs",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
    ]
