from django.apps import AppConfig


class MiniappConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "miniapp"

    def ready(self) -> None:
        from miniapp import checks  # noqa: F401
        from miniapp import recognition_signals  # noqa: F401

