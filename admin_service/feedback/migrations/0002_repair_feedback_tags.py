"""Reconcile the through table omitted by historical bot-first fake-initial.

Append-only migration: existing migration dependencies/state stay unchanged.
Creating the historical auto-through model gives it the real PK, pair uniqueness,
indexes and both FKs. Existing rows/table are never dropped or recreated.
"""
from django.db import migrations


def repair_feedback_tags(apps, schema_editor):
    feedback = apps.get_model("feedback", "FeedbackItem")
    through = feedback._meta.get_field("tags").remote_field.through
    connection = schema_editor.connection
    if connection.vendor == "postgresql":
        schema_editor.execute("SELECT pg_advisory_xact_lock(19481021, 3)")
    if through._meta.db_table not in connection.introspection.table_names():
        schema_editor.create_model(through)


class Migration(migrations.Migration):
    dependencies = [("feedback", "0001_initial")]
    operations = [migrations.RunPython(repair_feedback_tags, migrations.RunPython.noop)]
