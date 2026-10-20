from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0019_partnertrialclaim_activated_at")]
    operations = [
        migrations.CreateModel(
            name="DurableRegistrationAttribution",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("registered_at", models.DateTimeField()),
                ("source_event_id", models.BigIntegerField(blank=True, null=True, unique=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("partner_link", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="durable_registrations", to="miniapp.partnerlink")),
                ("user", models.OneToOneField(db_column="telegram_user_id", on_delete=django.db.models.deletion.CASCADE, related_name="durable_registration_attribution", to="users.telegramuser", to_field="tg_user_id")),
            ],
            options={"db_table": "durable_registration_attributions", "indexes": [models.Index(fields=("registered_at", "partner_link"), name="dur_reg_registered_link_idx")]},
        ),
    ]
