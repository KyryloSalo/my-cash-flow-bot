"""Keep the old bot INSERT contract when Django creates shared managed tables.

Only database defaults from the inspected bot_main.init_db DDL are transferred.
No table creation, row rewrite, nullability relaxation or history rewriting.
Python model defaults/auto_now aren't persistent SQL defaults.
"""
from django.db import migrations


SQL = (
    "ALTER TABLE bot_settings ALTER COLUMN value SET DEFAULT ''",
    "ALTER TABLE bot_settings ALTER COLUMN description SET DEFAULT ''",
    "ALTER TABLE bot_settings ALTER COLUMN value_type SET DEFAULT 'string'",
    'ALTER TABLE bot_settings ALTER COLUMN updated_at SET DEFAULT now()',
    "ALTER TABLE support_cases ALTER COLUMN subject SET DEFAULT ''",
    "ALTER TABLE support_cases ALTER COLUMN status SET DEFAULT 'new'",
    "ALTER TABLE support_cases ALTER COLUMN category SET DEFAULT 'other'",
    "ALTER TABLE support_cases ALTER COLUMN priority SET DEFAULT 'normal'",
    "ALTER TABLE support_cases ALTER COLUMN internal_notes SET DEFAULT ''",
    'ALTER TABLE support_cases ALTER COLUMN created_at SET DEFAULT now()',
    'ALTER TABLE support_cases ALTER COLUMN updated_at SET DEFAULT now()',
    "ALTER TABLE support_messages ALTER COLUMN attachment SET DEFAULT ''",
    'ALTER TABLE support_messages ALTER COLUMN created_at SET DEFAULT now()',
    "ALTER TABLE poll_campaigns ALTER COLUMN type SET DEFAULT 'rating'",
    "ALTER TABLE poll_campaigns ALTER COLUMN options SET DEFAULT '[]'::jsonb",
    "ALTER TABLE poll_campaigns ALTER COLUMN target_type SET DEFAULT 'all'",
    "ALTER TABLE poll_campaigns ALTER COLUMN manual_users SET DEFAULT '[]'::jsonb",
    "ALTER TABLE poll_campaigns ALTER COLUMN status SET DEFAULT 'draft'",
    'ALTER TABLE poll_campaigns ALTER COLUMN created_at SET DEFAULT now()',
    'ALTER TABLE poll_campaigns ALTER COLUMN updated_at SET DEFAULT now()',
    'ALTER TABLE poll_campaigns ALTER COLUMN total_recipients SET DEFAULT 0',
    'ALTER TABLE poll_campaigns ALTER COLUMN response_count SET DEFAULT 0',
    'ALTER TABLE poll_campaigns ALTER COLUMN low_rating_threshold SET DEFAULT 3',
    "ALTER TABLE poll_recipients ALTER COLUMN status SET DEFAULT 'pending'",
    "ALTER TABLE poll_recipients ALTER COLUMN error_message SET DEFAULT ''",
    'ALTER TABLE poll_recipients ALTER COLUMN created_at SET DEFAULT now()',
    "ALTER TABLE poll_responses ALTER COLUMN answer SET DEFAULT ''",
    "ALTER TABLE poll_responses ALTER COLUMN text_answer SET DEFAULT ''",
    "ALTER TABLE poll_responses ALTER COLUMN telegram_poll_id SET DEFAULT ''",
    'ALTER TABLE poll_responses ALTER COLUMN created_at SET DEFAULT now()',
    "ALTER TABLE feedback_items ALTER COLUMN source SET DEFAULT 'manual'",
    "ALTER TABLE feedback_items ALTER COLUMN category SET DEFAULT 'other'",
    "ALTER TABLE feedback_items ALTER COLUMN status SET DEFAULT 'new'",
    'ALTER TABLE feedback_items ALTER COLUMN created_at SET DEFAULT now()',
    'ALTER TABLE feedback_items ALTER COLUMN updated_at SET DEFAULT now()',
)


def preserve_runtime_defaults(apps, schema_editor):
    if schema_editor.connection.vendor == "postgresql":
        for statement in SQL:
            schema_editor.execute(statement)


class Migration(migrations.Migration):
    dependencies = [
        ("bot_settings", "0006_refresh_copy_defaults_after_audit"),
        ("support", "0001_initial"),
        ("polls", "0001_initial"),
        ("feedback", "0002_repair_feedback_tags"),
    ]
    operations = [migrations.RunPython(preserve_runtime_defaults, migrations.RunPython.noop)]
