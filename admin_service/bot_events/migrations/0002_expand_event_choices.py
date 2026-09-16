from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("bot_events", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="botevent",
            name="event_type",
            field=models.CharField(
                choices=[
                    ("start", "Start"),
                    ("new_user_registered", "New User Registered"),
                    ("onboarding_started", "Onboarding Started"),
                    ("onboarding_step_completed", "Onboarding Step Completed"),
                    ("onboarding_completed", "Onboarding Completed"),
                    ("onboarding_failed", "Onboarding Failed"),
                    ("onboarding_reset_by_admin", "Onboarding Reset By Admin"),
                    ("text_received", "Text Received"),
                    ("voice_received", "Voice Received"),
                    ("button_clicked", "Button Clicked"),
                    ("transaction_created", "Transaction Created"),
                    ("transaction_failed", "Transaction Failed"),
                    ("category_created", "Category Created"),
                    ("category_updated", "Category Updated"),
                    ("account_created", "Account Created"),
                    ("subscription_started", "Subscription Started"),
                    ("subscription_expired", "Subscription Expired"),
                    ("payment_success", "Payment Success"),
                    ("payment_failed", "Payment Failed"),
                    ("parse_error", "Parse Error"),
                    ("bot_error", "Bot Error"),
                    ("manual_message_sent", "Manual Message Sent"),
                    ("broadcast_completed", "Broadcast Completed"),
                    ("broadcast_failed", "Broadcast Failed"),
                    ("healthcheck_run", "Healthcheck Run"),
                ],
                max_length=64,
            ),
        ),
    ]
