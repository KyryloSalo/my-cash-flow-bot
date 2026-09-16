from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("users", "0008_alter_model_options")]

    operations = [
        migrations.AlterModelOptions(
            name="useradminstate",
            options={
                "permissions": [("can_use_test_tools", "Може використовувати тестові та QA-інструменти")],
                "verbose_name": "Стан користувача",
                "verbose_name_plural": "Стан користувачів",
            },
        ),
    ]
