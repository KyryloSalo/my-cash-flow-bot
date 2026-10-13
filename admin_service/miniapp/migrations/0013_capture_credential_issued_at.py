from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0012_capture_review_state")]
    operations = [migrations.AddField(
        model_name="captureconnection",
        name="credential_issued_at",
        field=models.DateTimeField(auto_now_add=True, default=django.utils.timezone.now),
        preserve_default=False,
    )]