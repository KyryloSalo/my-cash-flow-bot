from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="BrowserLoginTokenUse",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("token_hash", models.CharField(max_length=64, unique=True)),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("expires_at", models.DateTimeField(db_index=True)),
                ("used_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "miniapp_browser_login_token_uses",
                "verbose_name": "Mini App browser login token use",
                "verbose_name_plural": "Mini App browser login token uses",
                "ordering": ("-used_at",),
            },
        ),
    ]
