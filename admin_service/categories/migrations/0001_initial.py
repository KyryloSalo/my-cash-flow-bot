import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("users", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="CategoryTemplate",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("type", models.TextField()),
                ("name", models.TextField()),
                ("aliases", django.contrib.postgres.fields.ArrayField(base_field=models.TextField(), blank=True, default=list, size=None)),
                ("sort_order", models.IntegerField(default=0)),
                ("is_system", models.BooleanField(default=False)),
                ("is_active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField()),
                ("updated_at", models.DateTimeField()),
            ],
            options={
                "db_table": "category_templates",
                "managed": False,
                "verbose_name": "Category Template",
                "verbose_name_plural": "Category Templates",
                "ordering": ("type", "sort_order", "name"),
            },
        ),
        migrations.CreateModel(
            name="Category",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("kind", models.TextField(blank=True, null=True)),
                ("type", models.TextField(blank=True, null=True)),
                ("name", models.TextField()),
                ("aliases", django.contrib.postgres.fields.ArrayField(base_field=models.TextField(), blank=True, default=list, size=None)),
                ("source", models.TextField(default="legacy")),
                ("is_system", models.BooleanField(default=False)),
                ("is_active", models.BooleanField(default=True)),
                ("sort_order", models.IntegerField(default=0)),
                ("created_at", models.DateTimeField(blank=True, null=True)),
                ("updated_at", models.DateTimeField(blank=True, null=True)),
                ("deleted_at", models.DateTimeField(blank=True, null=True)),
                ("template", models.ForeignKey(blank=True, db_column="template_id", null=True, on_delete=models.deletion.SET_NULL, related_name="categories", to="categories.categorytemplate")),
                ("tg_user", models.ForeignKey(blank=True, db_column="tg_user_id", null=True, on_delete=models.deletion.DO_NOTHING, related_name="legacy_categories", to="users.telegramuser")),
                ("user", models.ForeignKey(blank=True, db_column="user_id", null=True, on_delete=models.deletion.DO_NOTHING, related_name="categories", to="users.telegramuser")),
            ],
            options={
                "db_table": "categories",
                "managed": False,
                "verbose_name": "Category",
                "verbose_name_plural": "Categories",
                "ordering": ("-is_system", "sort_order", "name"),
            },
        ),
    ]
