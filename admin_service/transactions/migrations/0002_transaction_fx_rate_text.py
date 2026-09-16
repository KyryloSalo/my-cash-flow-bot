from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("transactions", "0001_initial"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    DO $$
                    BEGIN
                      IF EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = current_schema() AND table_name = 'transactions'
                      ) THEN
                        ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fx_rate_text TEXT;
                      END IF;
                    END $$;
                    """,
                    reverse_sql=migrations.RunSQL.noop,
                )
            ],
            state_operations=[
                migrations.AddField(
                    model_name="transaction",
                    name="fx_rate_text",
                    field=models.TextField(blank=True, null=True),
                ),
            ],
        ),
    ]
