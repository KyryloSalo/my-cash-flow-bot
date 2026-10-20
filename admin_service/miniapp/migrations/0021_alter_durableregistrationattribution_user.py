import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("miniapp", "0020_durableregistrationattribution"),
        ("users", "0018_useroidcauthorizationflow_start_payload"),
    ]
    operations = [
        migrations.AlterField(
            model_name="durableregistrationattribution",
            name="user",
            field=models.OneToOneField(db_column="telegram_user_id", on_delete=django.db.models.deletion.CASCADE, related_name="durable_registration_attribution", to="users.telegramuser"),
        ),
    ]
