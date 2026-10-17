from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("transactions", "0004_alter_model_options")]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AddField(
                    model_name="transaction",
                    name="metadata",
                    field=models.JSONField(blank=True, default=dict),
                ),
            ],
            # transactions is an external/managed=False table. Its owner must
            # add the compatible JSON/JSONB column in the canonical schema.
            database_operations=[],
        ),
    ]
