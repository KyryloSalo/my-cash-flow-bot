from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("categories", "0001_initial")]

    operations = [
        migrations.AlterModelOptions(
            name="category",
            options={"managed": False, "ordering": ("-is_system", "sort_order", "name"), "verbose_name": "Категорія", "verbose_name_plural": "Категорії"},
        ),
        migrations.AlterModelOptions(
            name="categorytemplate",
            options={"managed": False, "ordering": ("type", "sort_order", "name"), "verbose_name": "Шаблон категорії", "verbose_name_plural": "Шаблони категорій"},
        ),
    ]
