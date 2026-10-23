from __future__ import annotations

from django.conf import settings
from django.utils import timezone

from admin_notifications.tasks import send_admin_notification_task
from miniapp.models import DurableRegistrationAttribution
from users.models import TelegramUser


def notify_new_registration(*, tg_user_id: int, source: str) -> None:
    """Run after commit so the immutable first-registration attribution is available."""
    if not settings.ADMIN_NOTIFICATIONS_ENABLED or not settings.TELEGRAM_BOT_TOKEN or not settings.ADMIN_TELEGRAM_IDS:
        return
    user = TelegramUser.objects.get(tg_user_id=tg_user_id)
    attribution = DurableRegistrationAttribution.objects.select_related("partner_link").filter(user=user).first()
    partner = attribution.partner_link if attribution else None
    username = f"@{user.username}" if user.username else "-"
    lines = [
        "Новий користувач", "",
        f"telegram_id: {user.tg_user_id}",
        f"username: {username}",
        f"name: {user.first_name or '-'}",
        f"time: {timezone.localtime(user.created_at).strftime('%Y-%m-%d %H:%M')}",
        f"Джерело: PWA / Telegram ({source})",
    ]
    payload = {"tg_user_id": user.tg_user_id, "source": source}
    if partner is not None:
        lines.extend([
            f"Партнер: {partner.name}",
            f"Код посилання: {partner.code}",
            f"Посилання: {partner.public_url}",
        ])
        payload.update({"partner_link_id": partner.pk, "partner_link_code": partner.code})
    else:
        lines.append("Посилання: прямий вхід (без партнерського посилання)")
    send_admin_notification_task.delay("new_user_registered", "\n".join(lines), payload)
