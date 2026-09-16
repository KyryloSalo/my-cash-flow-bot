from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("transactions", "0003_debt_models")]

    operations = [
        migrations.AlterModelOptions(
            name="transaction",
            options={"managed": False, "ordering": ("-created_at",), "verbose_name": "Транзакція", "verbose_name_plural": "Транзакції"},
        ),
    ]
