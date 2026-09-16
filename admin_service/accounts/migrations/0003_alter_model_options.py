from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("accounts", "0002_account_runtime_fields")]

    operations = [
        migrations.AlterModelOptions(
            name="account",
            options={"managed": False, "ordering": ("label",), "verbose_name": "Рахунок", "verbose_name_plural": "Рахунки"},
        ),
        migrations.AlterModelOptions(
            name="accountadminstate",
            options={"verbose_name": "Стан рахунку", "verbose_name_plural": "Стани рахунків"},
        ),
    ]
