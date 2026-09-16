from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("users", "0007_runtime_timestamp_defaults")]

    operations = [
        migrations.AlterModelOptions(name="adminnote", options={"ordering": ("-updated_at",), "verbose_name": "Нотатка адміна", "verbose_name_plural": "Нотатки адміна"}),
        migrations.AlterModelOptions(name="pushtopic", options={"ordering": ("name",), "verbose_name": "Тема повідомлень", "verbose_name_plural": "Теми повідомлень"}),
        migrations.AlterModelOptions(name="tag", options={"ordering": ("name",), "verbose_name": "Тег", "verbose_name_plural": "Теги"}),
        migrations.AlterModelOptions(name="telegramuser", options={"managed": False, "ordering": ("-created_at",), "verbose_name": "Користувач", "verbose_name_plural": "Користувачі"}),
        migrations.AlterModelOptions(name="useradminstate", options={"permissions": [("can_use_test_tools", "Може використовувати тестові та QA-інструменти")], "verbose_name": "Стан користувача", "verbose_name_plural": "Стани користувачів"}),
        migrations.AlterModelOptions(name="userpushtopic", options={"ordering": ("topic__name",), "verbose_name": "Тема користувача", "verbose_name_plural": "Теми користувачів"}),
        migrations.AlterModelOptions(name="usertag", options={"ordering": ("tag__name",), "verbose_name": "Тег користувача", "verbose_name_plural": "Теги користувачів"}),
    ]
