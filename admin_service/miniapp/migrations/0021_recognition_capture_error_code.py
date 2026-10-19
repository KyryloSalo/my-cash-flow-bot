from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0020_recognition_quality")]

    operations = [
        migrations.AddField(
            model_name="recognitioncapture",
            name="error_code",
            field=models.CharField(max_length=128, blank=True, default=""),
        ),
    ]
