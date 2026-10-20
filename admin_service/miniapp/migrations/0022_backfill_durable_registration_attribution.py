from django.db import migrations


def backfill(apps, schema_editor):
    Attribution = apps.get_model("miniapp", "DurableRegistrationAttribution")
    Event = apps.get_model("miniapp", "FunnelEvent")
    User = apps.get_model("users", "TelegramUser")
    # Only server-authored registration events are trustworthy.  If history was
    # purged, leave the user uncovered rather than inventing organic or partner
    # attribution from a later click/client payload.
    rows = (
        Event.objects.filter(event_name="registration_completed", source="server")
        .select_related("acquisition_session")
        .order_by("recorded_at", "id")
    )
    seen = set()
    for event in rows.iterator():
        user_id = event.acquisition_session.user_id
        if not user_id or user_id in seen:
            continue
        seen.add(user_id)
        Attribution.objects.get_or_create(
            user_id=user_id,
            defaults={
                "partner_link_id": event.first_partner_link_id,
                "registered_at": event.recorded_at,
                "source_event_id": event.pk,
            },
        )


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0021_alter_durableregistrationattribution_user")]
    operations = [migrations.RunPython(backfill, noop)]
