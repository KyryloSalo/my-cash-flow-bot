import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("subscriptions", "0003_monobank_billing"),
        ("users", "0005_useradminstate_access_scope"),
    ]

    operations = [
        migrations.CreateModel(
            name="PromoOffer",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("code", models.CharField(max_length=64, unique=True)),
                ("label", models.CharField(blank=True, default="", max_length=128)),
                ("trial_days", models.PositiveIntegerField(default=90)),
                ("source", models.CharField(blank=True, default="", max_length=128)),
                ("is_active", models.BooleanField(default=True)),
                ("max_uses", models.PositiveIntegerField(blank=True, null=True)),
                ("starts_at", models.DateTimeField(blank=True, null=True)),
                ("ends_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "promo_offers",
                "verbose_name": "Промо-офер",
                "verbose_name_plural": "Промо-офери",
                "ordering": ("code",),
            },
        ),
        migrations.AddField(
            model_name="subscription",
            name="promo_offer",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="subscriptions",
                to="subscriptions.promooffer",
            ),
        ),
        migrations.CreateModel(
            name="PromoOfferClaim",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "status",
                    models.CharField(
                        choices=[("pending", "Pending"), ("consumed", "Consumed"), ("ignored", "Ignored")],
                        default="pending",
                        max_length=32,
                    ),
                ),
                ("consumed_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "bind_payment",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="promo_claims",
                        to="subscriptions.payment",
                    ),
                ),
                (
                    "offer",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="claims",
                        to="subscriptions.promooffer",
                    ),
                ),
                (
                    "subscription",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="promo_claims",
                        to="subscriptions.subscription",
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        db_column="telegram_user_id",
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="promo_claims",
                        to="users.telegramuser",
                    ),
                ),
            ],
            options={
                "db_table": "promo_offer_claims",
                "verbose_name": "Заявка на промо-офер",
                "verbose_name_plural": "Заявки на промо-офери",
                "ordering": ("-updated_at",),
                "unique_together": {("offer", "user")},
            },
        ),
    ]
