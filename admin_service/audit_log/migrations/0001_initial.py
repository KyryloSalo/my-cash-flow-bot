from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="AdminAuditLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("action", models.CharField(max_length=128)),
                ("object_type", models.CharField(max_length=128)),
                ("object_id", models.CharField(blank=True, default="", max_length=255)),
                ("before", models.JSONField(blank=True, default=dict)),
                ("after", models.JSONField(blank=True, default=dict)),
                ("ip_address", models.CharField(blank=True, default="", max_length=128)),
                ("user_agent", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("admin_user", models.ForeignKey(blank=True, null=True, on_delete=models.deletion.SET_NULL, related_name="cashflow_audit_logs", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "db_table": "admin_audit_logs",
                "verbose_name": "Admin Audit Log",
                "verbose_name_plural": "Admin Audit Logs",
                "ordering": ("-created_at",),
            },
        ),
    ]
