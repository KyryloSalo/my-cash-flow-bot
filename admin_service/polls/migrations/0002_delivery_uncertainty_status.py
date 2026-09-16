from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("polls", "0001_initial")]

    operations = [
        migrations.AlterField(
            model_name="pollrecipient",
            name="status",
            field=models.CharField(
                choices=[
                    ("pending", "Pending"),
                    ("uncertain", "У роботі / результат невідомий (без автоматичного повтору)"),
                    ("sent", "Sent"),
                    ("failed", "Failed"),
                    ("blocked", "Blocked"),
                    ("responded", "Responded"),
                ],
                default="pending",
                max_length=32,
            ),
        ),
    ]
