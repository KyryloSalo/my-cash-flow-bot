from django.db import migrations, models


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS miniapp_install_nudge_states (
    id BIGSERIAL PRIMARY KEY,
    tg_user_id BIGINT NOT NULL UNIQUE,
    prompt_count SMALLINT NOT NULL DEFAULT 0 CHECK (prompt_count >= 0),
    telegram_reminder_count SMALLINT NOT NULL DEFAULT 0 CHECK (telegram_reminder_count >= 0),
    next_prompt_at TIMESTAMPTZ,
    next_telegram_reminder_at TIMESTAMPTZ,
    last_prompted_at TIMESTAMPTZ,
    last_telegram_reminder_at TIMESTAMPTZ,
    installed_at TIMESTAMPTZ,
    installed_platform VARCHAR(24) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_miniapp_install_nudges_due
ON miniapp_install_nudge_states (next_telegram_reminder_at)
WHERE installed_at IS NULL;
"""


class Migration(migrations.Migration):
    dependencies = [
        ("miniapp", "0002_write_receipt"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(CREATE_TABLE_SQL, reverse_sql=migrations.RunSQL.noop),
            ],
            state_operations=[
                migrations.CreateModel(
                    name="InstallNudgeState",
                    fields=[
                        ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                        ("tg_user_id", models.BigIntegerField(db_index=True, unique=True)),
                        ("prompt_count", models.PositiveSmallIntegerField(default=0)),
                        ("telegram_reminder_count", models.PositiveSmallIntegerField(default=0)),
                        ("next_prompt_at", models.DateTimeField(blank=True, null=True)),
                        ("next_telegram_reminder_at", models.DateTimeField(blank=True, null=True)),
                        ("last_prompted_at", models.DateTimeField(blank=True, null=True)),
                        ("last_telegram_reminder_at", models.DateTimeField(blank=True, null=True)),
                        ("installed_at", models.DateTimeField(blank=True, null=True)),
                        ("installed_platform", models.CharField(blank=True, default="", max_length=24)),
                        ("created_at", models.DateTimeField(auto_now_add=True)),
                        ("updated_at", models.DateTimeField(auto_now=True)),
                    ],
                    options={
                        "db_table": "miniapp_install_nudge_states",
                        "ordering": ("-updated_at",),
                    },
                ),
            ],
        ),
    ]
