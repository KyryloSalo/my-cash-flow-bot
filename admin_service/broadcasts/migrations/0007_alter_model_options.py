from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("broadcasts", "0006_delivery_uncertainty")]

    operations = [
        migrations.AlterModelOptions(
            name="adminmessagelog",
            options={"ordering": ("-created_at",), "verbose_name": "Лог повідомлення", "verbose_name_plural": "Логи повідомлень"},
        ),
    ]
