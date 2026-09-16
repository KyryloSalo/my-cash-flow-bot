from __future__ import annotations

from pathlib import Path
import json
from urllib import error, request

from celery.exceptions import CeleryError
from django.conf import settings
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.utils import timezone
from redis import Redis
from redis.exceptions import RedisError

from audit_log.models import AdminAuditLog
from bot_events.models import BotEvent
from config.celery import app as celery_app
from users.models import TelegramUser
from users.services import reset_user_onboarding


STATUS_OK = "OK"
STATUS_WARNING = "Попередження"
STATUS_FAILED = "Помилка"


def _check(label: str, status: str, detail: str) -> dict[str, str]:
    return {"label": label, "status": status, "detail": detail}


def _worst_status(checks: list[dict[str, str]]) -> str:
    if any(item["status"] == STATUS_FAILED for item in checks):
        return STATUS_FAILED
    if any(item["status"] == STATUS_WARNING for item in checks):
        return STATUS_WARNING
    return STATUS_OK


def _check_database() -> dict[str, str]:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    except Exception as exc:
        return _check("База даних", STATUS_FAILED, type(exc).__name__)
    return _check("База даних", STATUS_OK, "Підключення працює.")


def _check_redis() -> dict[str, str]:
    try:
        Redis.from_url(settings.REDIS_URL, socket_timeout=1, socket_connect_timeout=1).ping()
    except RedisError as exc:
        return _check("Redis", STATUS_FAILED, type(exc).__name__)
    return _check("Redis", STATUS_OK, "PING виконано успішно.")


def _check_worker() -> dict[str, str]:
    try:
        responses = celery_app.control.inspect(timeout=1.5).ping() or {}
    except CeleryError as exc:
        return _check("Worker", STATUS_FAILED, type(exc).__name__)
    if not responses:
        return _check("Worker", STATUS_WARNING, "Жоден Celery worker не відповів на ping.")
    return _check("Worker", STATUS_OK, f"Онлайн: {', '.join(sorted(responses.keys()))}")


def _check_telegram_api() -> dict[str, str]:
    if not settings.TELEGRAM_BOT_TOKEN:
        return _check("Telegram Bot API", STATUS_FAILED, "TELEGRAM_BOT_TOKEN не налаштований.")
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/getMe"
    req = request.Request(url, headers={"Accept": "application/json"})
    try:
        with request.urlopen(req, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except error.URLError as exc:
        return _check("Telegram Bot API", STATUS_FAILED, type(exc).__name__)
    except Exception as exc:
        return _check("Telegram Bot API", STATUS_FAILED, type(exc).__name__)
    if not payload.get("ok"):
        return _check("Telegram Bot API", STATUS_FAILED, str(payload.get("description") or "Невідома помилка Telegram API")[:300])
    username = payload.get("result", {}).get("username") or "-"
    return _check("Telegram Bot API", STATUS_OK, f"Доступний. username=@{username}")


def _check_static_files() -> dict[str, str]:
    static_root = Path(settings.STATIC_ROOT)
    if static_root.exists() and any(static_root.iterdir()):
        return _check("Статичні файли", STATUS_OK, str(static_root))
    return _check("Статичні файли", STATUS_WARNING, f"У {static_root} не знайдено файлів.")


def _check_pending_migrations() -> dict[str, str]:
    executor = MigrationExecutor(connection)
    plan = executor.migration_plan(executor.loader.graph.leaf_nodes())
    if plan:
        first_items = ", ".join(f"{migration.app_label}.{migration.name}" for migration, _ in plan[:5])
        return _check("Незапущені міграції", STATUS_WARNING, f"Є незастосовані міграції: {first_items}")
    return _check("Незапущені міграції", STATUS_OK, "Незастосованих міграцій немає.")


def _check_admin_domain() -> dict[str, str]:
    admin_domain = getattr(settings, "ADMIN_DOMAIN", "")
    if not admin_domain:
        return _check("ADMIN_DOMAIN", STATUS_FAILED, "ADMIN_DOMAIN порожній.")
    if admin_domain not in settings.ALLOWED_HOSTS:
        return _check("ADMIN_DOMAIN", STATUS_WARNING, f"{admin_domain} відсутній у ALLOWED_HOSTS.")
    return _check("ADMIN_DOMAIN", STATUS_OK, admin_domain)


def _check_https_and_debug() -> list[dict[str, str]]:
    https_status = STATUS_OK if settings.SECURE_SSL_REDIRECT else STATUS_WARNING
    debug_status = STATUS_OK if not settings.DEBUG else STATUS_FAILED
    return [
        _check("HTTPS у production", https_status, f"SECURE_SSL_REDIRECT={settings.SECURE_SSL_REDIRECT}"),
        _check("DEBUG у production", debug_status, f"DEBUG={settings.DEBUG}"),
    ]


def _check_token_exposure() -> dict[str, str]:
    if settings.TELEGRAM_BOT_TOKEN:
        return _check("TELEGRAM_BOT_TOKEN", STATUS_OK, "Токен налаштовано через env.")
    return _check("TELEGRAM_BOT_TOKEN", STATUS_WARNING, "Токен не налаштований.")


def _check_broadcast_queue(worker_check: dict[str, str], redis_check: dict[str, str]) -> dict[str, str]:
    if worker_check["status"] == STATUS_OK and redis_check["status"] == STATUS_OK:
        return _check("Черга розсилок", STATUS_OK, "Redis і worker доступні.")
    if worker_check["status"] == STATUS_FAILED or redis_check["status"] == STATUS_FAILED:
        return _check("Черга розсилок", STATUS_FAILED, "Залежності черги розсилок недоступні.")
    return _check("Черга розсилок", STATUS_WARNING, "Чергу розсилок не вдалося перевірити повністю.")


def _check_admin_notifications() -> dict[str, str]:
    if not settings.ADMIN_NOTIFICATIONS_ENABLED:
        return _check("Пуші адміну", STATUS_WARNING, "ADMIN_NOTIFICATIONS_ENABLED=false")
    if not settings.ADMIN_TELEGRAM_IDS:
        return _check("Пуші адміну", STATUS_WARNING, "ADMIN_TELEGRAM_IDS порожній.")
    if not settings.TELEGRAM_BOT_TOKEN:
        return _check("Пуші адміну", STATUS_FAILED, "TELEGRAM_BOT_TOKEN не налаштований.")
    return _check("Пуші адміну", STATUS_OK, f"Налаштовано chat id: {len(settings.ADMIN_TELEGRAM_IDS)}")


def _check_manual_message_service() -> dict[str, str]:
    if not settings.TELEGRAM_BOT_TOKEN:
        return _check("Ручні повідомлення", STATUS_FAILED, "TELEGRAM_BOT_TOKEN не налаштований.")
    return _check("Ручні повідомлення", STATUS_OK, "Повідомлення можна ставити в чергу через worker.")


def _check_audit_log_table() -> dict[str, str]:
    try:
        AdminAuditLog.objects.order_by("-created_at").first()
    except Exception as exc:
        return _check("Журнал дій адміна", STATUS_FAILED, type(exc).__name__)
    return _check("Журнал дій адміна", STATUS_OK, "Таблиця журналу дій доступна.")


def _check_bot_events_table(*, record_event: bool = False) -> dict[str, str]:
    try:
        if record_event:
            BotEvent.objects.create(
                user=None,
                event_type="healthcheck_run",
                source="admin",
                parsed_result={"time": timezone.localtime().isoformat()},
                success=True,
            )
        else:
            BotEvent.objects.order_by("-created_at").first()
    except Exception as exc:
        return _check("Події бота", STATUS_FAILED, type(exc).__name__)
    return _check("Події бота", STATUS_OK, "Таблиця подій бота доступна.")


def _pick_dry_run_reset_target() -> TelegramUser | None:
    allowlisted_ids = list(getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []))
    if allowlisted_ids:
        user = TelegramUser.objects.filter(tg_user_id__in=allowlisted_ids).order_by("tg_user_id").first()
        if user is not None:
            return user
    return TelegramUser.objects.filter(admin_state__is_test_user=True).order_by("tg_user_id").first()


def _check_reset_service_dry_run(admin_user=None) -> dict[str, str]:
    user = _pick_dry_run_reset_target()
    if user is None:
        return _check("Скидання онбордингу (сухий прогін)", STATUS_WARNING, "Не налаштовано тестового користувача для сухого прогону.")
    try:
        result = reset_user_onboarding(
            user.tg_user_id,
            "safe",
            admin_user=admin_user,
            reason="healthcheck dry run",
            options={"dry_run": True, "double_confirmed": True},
        )
    except Exception as exc:
        return _check("Скидання онбордингу (сухий прогін)", STATUS_FAILED, type(exc).__name__)
    return _check("Скидання онбордингу (сухий прогін)", STATUS_OK, f"Сухий прогін успішний для telegram_id={result['user_id']}")


def _check_admin_modules() -> dict[str, str]:
    required_tables = [
        "user_admin_states",
        "broadcasts",
        "broadcast_recipients",
        "bot_events",
        "admin_audit_logs",
        "admin_notification_logs",
    ]
    existing_tables = set(connection.introspection.table_names())
    missing = [table for table in required_tables if table not in existing_tables]
    if missing:
        return _check("Стан адмін-модулів", STATUS_WARNING, f"Відсутні таблиці: {', '.join(missing)}")
    return _check("Стан адмін-модулів", STATUS_OK, "Основні адмін-таблиці на місці.")


def _safe_check(label, function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except Exception as exc:
        # Exceptions may contain connection DSNs or Telegram URLs: type only.
        return _check(label, STATUS_FAILED, type(exc).__name__)


def _check_scheduler():
    from common.readiness import _beat
    return _check("Beat", STATUS_OK if _beat() else STATUS_FAILED, "Scheduler heartbeat")


def run_admin_healthcheck(*, admin_user=None, record_event: bool = False) -> dict:
    database_check = _safe_check("Database", _check_database)
    redis_check = _safe_check("Redis", _check_redis)
    worker_check = _safe_check("Worker", _check_worker)
    checks = [database_check, redis_check, worker_check]
    for label, function in [
        ("Telegram API", _check_telegram_api),
        ("Static", _check_static_files),
        ("Migrations", _check_pending_migrations),
        ("Domain", _check_admin_domain),
        ("Token", _check_token_exposure),
        ("Notifications", _check_admin_notifications),
        ("Messages", _check_manual_message_service),
        ("Audit", _check_audit_log_table),
        ("Modules", _check_admin_modules),
        ("Beat", _check_scheduler),
    ]:
        checks.append(_safe_check(label, function))
    try:
        checks.extend(_check_https_and_debug())
    except Exception as exc:
        checks.append(_check("Security", STATUS_FAILED, type(exc).__name__))
    checks.append(_safe_check("Queue", _check_broadcast_queue, worker_check, redis_check))
    checks.append(_safe_check("Events", _check_bot_events_table, record_event=record_event))
    checks.append(_safe_check("Reset preview", _check_reset_service_dry_run, admin_user=admin_user))
    return {
        "title": "Перевірка системи",
        "summary_status": _worst_status(checks),
        "generated_at": timezone.localtime(),
        "checks": checks,
    }
