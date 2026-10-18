from django.db import migrations, models
from django.db.models import Exists, OuterRef, Q
from django.db.models.functions import Now


def mark_legacy_claims(apps, schema_editor):
    # A legacy claim is consumed only when the bind produced a trial with a
    # currently bound card.  Cardless partner trials were reservations and
    # must remain pending for the first confirmed bind.
    Claim = apps.get_model("miniapp", "PartnerTrialClaim")
    Subscription = apps.get_model("subscriptions", "Subscription")
    BillingProfile = apps.get_model("subscriptions", "BillingProfile")
    bound_card = BillingProfile.objects.filter(
        user_id=OuterRef("user_id"),
        status="active",
    ).exclude(card_token="")
    granted_access = Subscription.objects.filter(user_id=OuterRef("user_id")).filter(
        ~Q(status="trial") | Exists(bound_card),
    )
    Claim.objects.filter(Exists(granted_access)).update(activated_at=Now())


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0018_acquisitionsession_partner_offer_snapshot"), ("subscriptions", "0011_billingconsent_canary_owned_and_more")]
    operations = [
        migrations.AddField(
            model_name="partnertrialclaim",
            name="activated_at",
            field=models.DateTimeField(blank=True, null=True, db_index=True),
        ),
        migrations.RunPython(mark_legacy_claims, migrations.RunPython.noop),
    ]
