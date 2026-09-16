from django.apps import AppConfig
from django.db.models.signals import pre_migrate


class UsersConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "users"

    def ready(self):
        # Registration only: do not query/write a DB during app initialization.
        from common.runtime_schema import bootstrap_before_migrate

        pre_migrate.connect(
            bootstrap_before_migrate,
            sender=self,
            dispatch_uid="users.bootstrap_runtime_schema",
        )
