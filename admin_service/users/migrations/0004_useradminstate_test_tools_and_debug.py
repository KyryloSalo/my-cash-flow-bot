from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("users", "0003_seed_push_topics"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="telegramuser",
            name="last_name",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS last_name TEXT;",
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="current_fsm_state",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="is_test_user",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="last_admin_reset_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="last_bot_response",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="last_onboarding_event_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="last_parse_error",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="last_user_input",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="marked_as_test_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="marked_as_test_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="marked_test_users",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="onboarding_payload",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="pending_admin_reset_mode",
            field=models.CharField(blank=True, default="", max_length=32),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="pending_admin_reset_requested_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="useradminstate",
            name="test_user_notes",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AlterModelOptions(
            name="useradminstate",
            options={
                "db_table": "user_admin_states",
                "verbose_name": "User Admin State",
                "verbose_name_plural": "User Admin States",
                "permissions": [("can_use_test_tools", "Can use admin test and QA tools")],
            },
        ),
    ]
