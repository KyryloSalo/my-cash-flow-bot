from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [('miniapp', '0023_merge_recognition_and_registration')]

    operations = [
        migrations.AddField(
            model_name='recognitioncaptureconfig',
            name='all_images_enabled',
            field=models.BooleanField(default=False),
        ),
    ]
