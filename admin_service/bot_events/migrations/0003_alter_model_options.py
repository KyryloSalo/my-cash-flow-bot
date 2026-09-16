from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("bot_events", "0002_expand_event_choices")]

    operations = [
        migrations.AlterModelOptions(
            name="botevent",
            options={"ordering": ("-created_at",), "verbose_name": "Подія бота", "verbose_name_plural": "Події бота"},
        ),
    ]
