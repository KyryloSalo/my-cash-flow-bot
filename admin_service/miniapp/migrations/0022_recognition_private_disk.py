from django.db import migrations, models


def refuse_unmigrated_blobs(apps, schema_editor):
    captures = apps.get_model('miniapp', 'RecognitionCapture')
    if captures.objects.using(schema_editor.connection.alias).exclude(raw_media=None).exists():
        raise RuntimeError(
            'Legacy recognition media exists; migrate it to private storage before applying 0022. '
            'Refusing to discard raw inputs.'
        )


class Migration(migrations.Migration):
    dependencies = [('miniapp', '0021_recognition_capture_error_code')]

    operations = [
        migrations.RunPython(refuse_unmigrated_blobs, migrations.RunPython.noop),
        migrations.RemoveField(model_name='recognitioncapture', name='raw_media'),
        migrations.AddField(model_name='recognitioncapture', name='raw_media',
                            field=models.CharField(max_length=64, blank=True, default='')),
        migrations.AddField(model_name='recognitioncapture', name='raw_media_size',
                            field=models.PositiveBigIntegerField(default=0)),
        migrations.AlterField(model_name='recognitioncaptureconfig', name='max_total_bytes',
                              field=models.PositiveBigIntegerField(default=1024 * 1024 * 1024)),
        migrations.AlterField(model_name='recognitioncaptureconfig', name='retention_days',
                              field=models.PositiveIntegerField(default=7)),
        migrations.AddField(model_name='recognitioncaptureconfig', name='storage_warning_at',
                            field=models.DateTimeField(null=True, blank=True)),
        migrations.AddField(model_name='recognitioncaptureconfig', name='storage_full_at',
                            field=models.DateTimeField(null=True, blank=True)),
    ]
