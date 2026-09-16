from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("users", "0011_userauthsession")]

    operations = [
        migrations.CreateModel(
            name="UserOidcTokenUse",
            fields=[
                ("token_hash", models.CharField(max_length=64, primary_key=True, serialize=False)),
                ("provider", models.CharField(max_length=32)),
                ("subject", models.CharField(max_length=255)),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("expires_at", models.DateTimeField(db_index=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={"db_table": "user_oidc_token_uses"},
        ),
    ]
