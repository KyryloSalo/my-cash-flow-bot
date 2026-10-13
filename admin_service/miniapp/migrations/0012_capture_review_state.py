from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0011_capture_foundation")]

    operations = [
        migrations.AddField(
            model_name="captureevent",
            name="draft_id",
            field=models.CharField(blank=True, max_length=128, null=True, unique=True),
        ),
        migrations.AddField(
            model_name="captureevent",
            name="draft_payload",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
