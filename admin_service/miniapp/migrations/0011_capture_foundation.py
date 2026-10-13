from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0010_funnelevent_first_partner_link_and_more")]

    operations = [
        migrations.CreateModel(
            name="CaptureConnection",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("platform", models.CharField(max_length=40)),
                ("status", models.CharField(default="pending", max_length=24)),
                ("credential_hash", models.CharField(max_length=128)),
                ("last_received_at", models.DateTimeField(blank=True, null=True)),
                ("revoked_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "miniapp_capture_connections"},
        ),
        migrations.CreateModel(
            name="CaptureEvent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("event_id", models.UUIDField()),
                ("source", models.CharField(max_length=48)),
                ("bank", models.CharField(max_length=32)),
                ("observed_at", models.DateTimeField()),
                ("occurred_at", models.DateTimeField(blank=True, null=True)),
                ("amount", models.DecimalField(blank=True, decimal_places=2, max_digits=18, null=True)),
                ("currency", models.CharField(blank=True, max_length=3, null=True)),
                ("merchant", models.CharField(blank=True, max_length=160, null=True)),
                ("operation_hint", models.CharField(max_length=16)),
                ("parser_version", models.CharField(max_length=64)),
                ("payload", models.JSONField(default=dict)),
                ("payload_fingerprint", models.CharField(max_length=64)),
                ("status", models.CharField(default="pending", max_length=24)),
                ("source_revision", models.PositiveIntegerField(default=1)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("connection", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="events", to="miniapp.captureconnection")),
            ],
            options={"db_table": "miniapp_capture_events"},
        ),
        migrations.AddConstraint(
            model_name="captureevent",
            constraint=models.UniqueConstraint(fields=("connection", "event_id"), name="miniapp_capture_event_unique"),
        ),
    ]
