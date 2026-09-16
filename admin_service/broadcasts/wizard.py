from __future__ import annotations

import json
from datetime import timedelta

from django import forms
from django.utils import timezone

from broadcasts.models import Broadcast, Segment
from broadcasts.targets import InvalidAudience, parse_manual_user_ids, users_for_target
from users.models import PushTopic, Tag


WIZARD_STEPS = (
    (1, "Повідомлення"),
    (2, "Отримувачі"),
    (3, "Перевірка"),
    (4, "Відправка"),
)

BROADCAST_TEST_SESSION_KEY = "cashflow_admin_broadcast_test"
BROADCAST_TEST_TTL = timedelta(hours=2)
LARGE_BROADCAST_CONFIRM_THRESHOLD = 20

WIZARD_SEGMENT_DEFAULTS = {
    "all_users": {
        "name": "All Users",
        "type": "system",
        "description": "All users",
        "is_active": True,
        "is_system": True,
    },
    "paid_users": {
        "name": "Paid Users",
        "type": "system",
        "description": "Users with paid/manual/lifetime access",
        "is_active": True,
        "is_system": True,
    },
    "trial_users": {
        "name": "Trial Users",
        "type": "system",
        "description": "Users on trial",
        "is_active": True,
        "is_system": True,
    },
    "free_users": {
        "name": "Free Users",
        "type": "system",
        "description": "Users without subscription",
        "is_active": True,
        "is_system": True,
    },
    "inactive_30d": {
        "name": "Inactive 30d",
        "type": "system",
        "description": "Not seen for 30 days",
        "is_active": True,
        "is_system": True,
    },
    "test_users": {
        "name": "Тестові користувачі",
        "type": "system",
        "description": "Користувачі з позначкою тестового або додані до allowlist для QA",
        "is_active": True,
        "is_system": True,
    },
    "users_by_tag": {
        "name": "Users by Tag",
        "type": "tag",
        "description": "Segment resolved through target tag",
        "is_active": True,
        "is_system": True,
    },
}

SEGMENT_GROUP_BY_SLUG = {
    "all_users": "all",
    "paid_users": "paid",
    "trial_users": "trial",
    "free_users": "free",
    "inactive_30d": "inactive",
    "test_users": "test",
}

GROUP_LABELS = {
    "all": "Усі активні користувачі",
    "paid": "Тільки платні",
    "trial": "Тільки тріал",
    "free": "Тільки безкоштовні",
    "inactive": "Тільки неактивні",
    "test": "Тільки тестові користувачі",
    "tag": "Користувачі з тегом",
    "manual": "Обрані вручну",
    "topic": "Користувачі з темою повідомлень",
}


class BroadcastWizardMessageForm(forms.ModelForm):
    parse_mode = forms.ChoiceField(
        choices=[
            (Broadcast.ParseMode.NONE, "Звичайний текст"),
            (Broadcast.ParseMode.HTML, "HTML"),
            (Broadcast.ParseMode.MARKDOWN, "Markdown"),
        ],
        label="Формат",
    )

    class Meta:
        model = Broadcast
        fields = ("title", "message_text", "parse_mode", "button_text", "button_url")
        labels = {
            "title": "Назва розсилки",
            "message_text": "Текст повідомлення",
            "button_text": "Текст кнопки",
            "button_url": "URL кнопки",
        }
        widgets = {
            "message_text": forms.Textarea(attrs={"rows": 10}),
        }

    def clean(self):
        cleaned_data = super().clean()
        button_text = (cleaned_data.get("button_text") or "").strip()
        button_url = (cleaned_data.get("button_url") or "").strip()
        if button_text and not button_url:
            self.add_error("button_url", "Якщо вказаний текст кнопки, потрібен і URL.")
        if button_url and not button_text:
            self.add_error("button_text", "Якщо вказаний URL кнопки, потрібен і текст кнопки.")
        return cleaned_data


class BroadcastWizardRecipientsForm(forms.Form):
    recipient_group = forms.ChoiceField(
        label="Кого отримає розсилка",
        widget=forms.RadioSelect,
        choices=tuple((key, label) for key, label in GROUP_LABELS.items()),
    )
    target_tag = forms.ModelChoiceField(queryset=Tag.objects.none(), required=False, label="Тег")
    target_topic = forms.ModelChoiceField(queryset=PushTopic.objects.none(), required=False, label="Тема повідомлень")
    manual_users = forms.CharField(
        required=False,
        label="Telegram ID користувачів",
        help_text="Вкажіть Telegram ID через кому або з нового рядка.",
        widget=forms.Textarea(attrs={"rows": 6}),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["target_tag"].queryset = Tag.objects.order_by("name")
        self.fields["target_topic"].queryset = PushTopic.objects.filter(is_active=True).order_by("name")

    def clean(self):
        cleaned_data = super().clean()
        group = cleaned_data.get("recipient_group")
        if group == "tag" and cleaned_data.get("target_tag") is None:
            self.add_error("target_tag", "Оберіть тег.")
        if group == "topic" and cleaned_data.get("target_topic") is None:
            self.add_error("target_topic", "Оберіть тему повідомлень.")
        if group == "manual":
            manual_ids = parse_manual_user_ids(cleaned_data.get("manual_users"))
            if not manual_ids:
                self.add_error("manual_users", "Вкажіть хоча б один коректний Telegram ID.")
            cleaned_data["manual_ids"] = manual_ids
        return cleaned_data


class BroadcastWizardLaunchForm(forms.Form):
    confirmation_text = forms.CharField(required=False, label='Для великої розсилки введіть "SEND"')


def ensure_wizard_segments() -> None:
    for slug, defaults in WIZARD_SEGMENT_DEFAULTS.items():
        Segment.objects.get_or_create(slug=slug, defaults=defaults)


def is_editable_broadcast(broadcast: Broadcast) -> bool:
    return broadcast.status in {Broadcast.Status.DRAFT, Broadcast.Status.FAILED, Broadcast.Status.CANCELLED}


def broadcast_config_fingerprint(broadcast: Broadcast | None) -> tuple:
    if broadcast is None:
        return ()
    return (
        broadcast.title,
        broadcast.message_text,
        broadcast.image_url,
        broadcast.parse_mode,
        broadcast.button_text,
        broadcast.button_url,
        broadcast.target_type,
        broadcast.target_segment_id,
        broadcast.target_tag_id,
        broadcast.target_topic_id,
        tuple(parse_manual_user_ids(broadcast.manual_users)),
        broadcast.segment_filter,
    )


def recipient_group_from_broadcast(broadcast: Broadcast | None) -> str:
    if broadcast is None:
        return "all"
    if broadcast.target_type == Broadcast.TargetType.ALL:
        return "all"
    if broadcast.target_type == Broadcast.TargetType.MANUAL_USERS:
        return "manual"
    if broadcast.target_type == Broadcast.TargetType.TAG:
        return "tag"
    if broadcast.target_type == Broadcast.TargetType.PUSH_TOPIC:
        return "topic"
    if broadcast.target_type == Broadcast.TargetType.SEGMENT and broadcast.target_segment:
        return SEGMENT_GROUP_BY_SLUG.get(broadcast.target_segment.slug, "")
    return ""


def recipients_form_initial(broadcast: Broadcast | None) -> dict:
    if broadcast is None:
        return {"recipient_group": "all"}
    return {
        "recipient_group": recipient_group_from_broadcast(broadcast),
        "target_tag": broadcast.target_tag_id,
        "target_topic": broadcast.target_topic_id,
        "manual_users": "\n".join(str(item) for item in parse_manual_user_ids(broadcast.manual_users)),
    }


def apply_recipients_form(broadcast: Broadcast, cleaned_data: dict) -> None:
    group = cleaned_data["recipient_group"]
    if group not in GROUP_LABELS:
        raise InvalidAudience("Unknown recipient group; select an explicit audience.")
    ensure_wizard_segments()
    broadcast.target_segment = None
    broadcast.target_tag = None
    broadcast.target_topic = None
    broadcast.manual_users = []
    broadcast.segment_filter = {}

    if group == "all":
        broadcast.target_type = Broadcast.TargetType.ALL
        return
    if group == "tag":
        broadcast.target_type = Broadcast.TargetType.TAG
        broadcast.target_tag = cleaned_data["target_tag"]
        return
    if group == "manual":
        broadcast.target_type = Broadcast.TargetType.MANUAL_USERS
        broadcast.manual_users = cleaned_data.get("manual_ids") or parse_manual_user_ids(cleaned_data.get("manual_users"))
        return
    if group == "topic":
        broadcast.target_type = Broadcast.TargetType.PUSH_TOPIC
        broadcast.target_topic = cleaned_data["target_topic"]
        return

    slug = {
        "paid": "paid_users",
        "trial": "trial_users",
        "free": "free_users",
        "inactive": "inactive_30d",
        "test": "test_users",
    }[group]
    broadcast.target_type = Broadcast.TargetType.SEGMENT
    broadcast.target_segment = Segment.objects.get(slug=slug)


def recipients_queryset_for_broadcast(broadcast: Broadcast):
    return users_for_target(
        target_type=broadcast.target_type,
        target_segment=broadcast.target_segment,
        target_tag=broadcast.target_tag,
        target_topic=broadcast.target_topic,
        manual_users=broadcast.manual_users or broadcast.segment_filter.get("telegram_ids", []),
    )


def recipient_preview_data(broadcast: Broadcast, *, limit: int = 20) -> dict:
    try:
        queryset = recipients_queryset_for_broadcast(broadcast)
    except InvalidAudience as exc:
        return {"count": 0, "users": [], "audience_ids": [], "error": str(exc)}
    users = list(queryset[:limit])
    audience_ids = sorted(set(queryset.values_list("tg_user_id", flat=True)))
    return {
        "count": len(audience_ids),
        "users": users,
        "audience_ids": audience_ids,
    }


def delivery_preview_rows(broadcast: Broadcast, *, limit: int = 20):
    return list(broadcast.recipients.select_related("user").order_by("-id")[:limit])


def broadcast_test_fingerprint(broadcast: Broadcast) -> list:
    preview = recipient_preview_data(broadcast, limit=20)
    # Django's session serializer converts every tuple (including nested ones).
    # Compare JSON-native values on both sides of the request boundary.
    return json.loads(json.dumps(broadcast_config_fingerprint(broadcast))) + [preview["count"], preview.get("audience_ids", [])]


def remember_broadcast_test(request, broadcast: Broadcast) -> None:
    request.session[BROADCAST_TEST_SESSION_KEY] = {
        "broadcast_id": broadcast.pk,
        "admin_user_id": request.user.pk,
        "created_at": timezone.now().isoformat(),
        "fingerprint": broadcast_test_fingerprint(broadcast),
    }
    request.session.modified = True


def broadcast_test_matches(request, broadcast: Broadcast) -> bool:
    payload = request.session.get(BROADCAST_TEST_SESSION_KEY)
    if not payload:
        return False
    if payload.get("broadcast_id") != broadcast.pk or payload.get("admin_user_id") != request.user.pk:
        return False
    try:
        created_at = timezone.datetime.fromisoformat(payload["created_at"])
    except Exception:
        return False
    if timezone.is_naive(created_at):
        created_at = timezone.make_aware(created_at, timezone.get_current_timezone())
    if timezone.now() - created_at > BROADCAST_TEST_TTL:
        return False
    return payload.get("fingerprint") == broadcast_test_fingerprint(broadcast)
