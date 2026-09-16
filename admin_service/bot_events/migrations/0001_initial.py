from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("users", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="BotEvent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("event_type", models.CharField(choices=[("start", "Start"), ("new_user_registered", "New User Registered"), ("onboarding_started", "Onboarding Started"), ("onboarding_completed", "Onboarding Completed"), ("text_received", "Text Received"), ("voice_received", "Voice Received"), ("button_clicked", "Button Clicked"), ("transaction_created", "Transaction Created"), ("transaction_failed", "Transaction Failed"), ("category_created", "Category Created"), ("category_updated", "Category Updated"), ("account_created", "Account Created"), ("subscription_started", "Subscription Started"), ("subscription_expired", "Subscription Expired"), ("payment_success", "Payment Success"), ("payment_failed", "Payment Failed"), ("parse_error", "Parse Error"), ("bot_error", "Bot Error"), ("manual_message_sent", "Manual Message Sent"), ("broadcast_completed", "Broadcast Completed"), ("broadcast_failed", "Broadcast Failed")], max_length=64)),
                ("source", models.CharField(blank=True, default="", max_length=64)),
                ("raw_input", models.TextField(blank=True, default="")),
                ("parsed_result", models.JSONField(blank=True, default=dict)),
                ("success", models.BooleanField(default=True)),
                ("error_message", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("user", models.ForeignKey(blank=True, db_column="telegram_user_id", null=True, on_delete=models.deletion.SET_NULL, related_name="bot_events", to="users.telegramuser")),
            ],
            options={
                "db_table": "bot_events",
                "verbose_name": "Bot Event",
                "verbose_name_plural": "Bot Events",
                "ordering": ("-created_at",),
            },
        ),
    ]
