from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("users", "0016_activate_synthetic_canary_identity")]

    operations = [
        migrations.CreateModel(
            name="UserOidcAuthorizationFlow",
            fields=[
                ("state_hash", models.CharField(max_length=64, primary_key=True, serialize=False)),
                ("nonce", models.CharField(max_length=128)),
                ("code_verifier", models.CharField(max_length=128)),
                ("return_to", models.TextField(default="/app/")),
                ("issued_at", models.DateTimeField()),
                ("expires_at", models.DateTimeField(db_index=True)),
            ],
            options={"db_table": "user_oidc_authorization_flows"},
        ),
    ]