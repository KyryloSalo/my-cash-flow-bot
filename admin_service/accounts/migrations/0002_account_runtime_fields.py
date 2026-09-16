from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("accounts", "0001_initial"),
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
                        WHERE table_schema = current_schema() AND table_name = 'accounts'
                      ) THEN
                        ALTER TABLE accounts ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'other';
                        ALTER TABLE accounts ADD COLUMN IF NOT EXISTS balance NUMERIC(18,2) NOT NULL DEFAULT 0;
                        ALTER TABLE accounts ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT true;
                        ALTER TABLE accounts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

                        UPDATE accounts
                        SET account_type = CASE
                          WHEN lower(label) LIKE '%%готівка%%' OR lower(label) LIKE '%%cash%%' THEN 'cash'
                          ELSE 'card'
                        END
                        WHERE COALESCE(account_type, '') IN ('', 'other');

                        IF EXISTS (
                          SELECT 1
                          FROM information_schema.tables
                          WHERE table_schema = current_schema() AND table_name = 'transactions'
                        ) THEN
                          WITH ledger AS (
                            SELECT
                              a.id,
                              COALESCE(a.starting_balance, 0)
                              + COALESCE(SUM(CASE
                                  WHEN t.account_id = a.id AND t.type = 'income' THEN t.amount
                                  ELSE 0
                                END), 0)
                              - COALESCE(SUM(CASE
                                  WHEN t.account_id = a.id AND t.type = 'expense' THEN t.amount
                                  ELSE 0
                                END), 0)
                              - COALESCE(SUM(CASE
                                  WHEN t.from_account_id = a.id AND t.type = 'transfer' THEN t.amount
                                  ELSE 0
                                END), 0)
                              + COALESCE(SUM(CASE
                                  WHEN t.to_account_id = a.id AND t.type = 'transfer' THEN COALESCE(t.to_amount, t.amount)
                                  ELSE 0
                                END), 0) AS computed_balance
                            FROM accounts a
                            LEFT JOIN transactions t
                              ON t.tg_user_id = a.tg_user_id
                             AND (
                               t.account_id = a.id
                               OR t.from_account_id = a.id
                               OR t.to_account_id = a.id
                             )
                            GROUP BY a.id, a.starting_balance
                          )
                          UPDATE accounts AS a
                          SET balance = ledger.computed_balance,
                              updated_at = now()
                          FROM ledger
                          WHERE ledger.id = a.id;
                        ELSE
                          UPDATE accounts
                          SET balance = COALESCE(starting_balance, 0),
                              updated_at = now()
                          WHERE balance = 0;
                        END IF;
                      END IF;
                    END $$;
                    """,
                    reverse_sql=migrations.RunSQL.noop,
                )
            ],
            state_operations=[
                migrations.AddField(
                    model_name="account",
                    name="account_type",
                    field=models.TextField(default="other"),
                ),
                migrations.AddField(
                    model_name="account",
                    name="balance",
                    field=models.DecimalField(decimal_places=2, default=0, max_digits=18),
                ),
                migrations.AddField(
                    model_name="account",
                    name="is_active",
                    field=models.BooleanField(default=True),
                ),
                migrations.AddField(
                    model_name="account",
                    name="updated_at",
                    field=models.DateTimeField(),
                ),
            ],
        ),
    ]
