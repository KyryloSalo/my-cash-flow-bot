from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0014_acquisitionsession_canary_owned")]

    operations = [
        migrations.AddField(
            model_name="browserlogintokenuse",
            name="purpose",
            field=models.CharField(default="ordinary_browser_login", db_index=True, max_length=64),
        ),
    ]
