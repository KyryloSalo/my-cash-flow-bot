from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("subscriptions", "0007_recovery_delivery_claims")]

    operations = [
        migrations.AlterModelOptions(name="payment", options={"ordering": ("-created_at",), "verbose_name": "Платіж", "verbose_name_plural": "Платежі"}),
        migrations.AlterModelOptions(name="plan", options={"ordering": ("display_order", "name"), "verbose_name": "Тарифний план", "verbose_name_plural": "Тарифні плани"}),
        migrations.AlterModelOptions(name="subscription", options={"ordering": ("-created_at",), "verbose_name": "Підписка", "verbose_name_plural": "Підписки"}),
        migrations.AlterModelOptions(name="subscriptionevent", options={"ordering": ("-created_at",), "verbose_name": "Подія підписки", "verbose_name_plural": "Події підписки"}),
    ]
