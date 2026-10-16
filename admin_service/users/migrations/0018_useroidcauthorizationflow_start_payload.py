from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("users", "0017_useroidcauthorizationflow")]

    operations = [
        migrations.AddField(
            model_name="useroidcauthorizationflow",
            name="start_payload",
            field=models.CharField(blank=True, default="", max_length=32),
        ),
    ]
