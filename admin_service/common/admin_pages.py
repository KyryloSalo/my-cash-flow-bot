from __future__ import annotations

from datetime import datetime, timedelta

from django import forms
from django.conf import settings
from django.contrib import messages
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.template.response import TemplateResponse
from django.urls import reverse
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from admin_notifications.tasks import send_admin_notification_task
from bot_events.models import BotEvent
from broadcasts.models import AdminMessageLog, Broadcast, Segment
from broadcasts.tasks import send_broadcast_task, send_manual_message_task
from common.admin_actions import can_admin_action, require_admin_action
from common.audit import create_audit_log
from common.healthcheck import _check_telegram_api, run_admin_healthcheck
from users.models import TelegramUser, UserAdminState
from users.services import RESET_MODES, ResetOnboardingError, reset_user_onboarding


MANUAL_MESSAGE_TEST_SESSION_KEY = "cashflow_admin_manual_message_test"
MANUAL_MESSAGE_TEST_TTL = timedelta(hours=1)
ONBOARDING_EVENT_TYPES = [
    "start",
    "onboarding_started",
    "onboarding_step_completed",
    "onboarding_completed",
    "onboarding_failed",
    "onboarding_reset_by_admin",
    "text_received",
    "voice_received",
    "parse_error",
    "bot_error",
]
PARSE_MODE_CHOICES = [
    (Broadcast.ParseMode.NONE, "Звичайний текст"),
    (Broadcast.ParseMode.HTML, "HTML"),
    (Broadcast.ParseMode.MARKDOWN, "Markdown"),
]


def can_use_test_tools(user) -> bool:
    return bool(
        user.is_active
        and user.is_staff
        and settings.ENABLE_ADMIN_TEST_TOOLS
        and can_admin_action(user, "qa_tools")
    )


def _require_staff(request) -> None:
    if not request.user.is_staff:
        raise PermissionDenied


def _require_test_tools(request) -> None:
    if not settings.ENABLE_ADMIN_TEST_TOOLS:
        raise PermissionDenied("ENABLE_ADMIN_TEST_TOOLS=false")
    if not can_use_test_tools(request.user):
        raise PermissionDenied("Ви не маєте доступу до QA-інструментів.")


def _user_option_label(user: TelegramUser) -> str:
    parts = [str(user.tg_user_id)]
    if user.username:
        parts.append(f"@{user.username}")
    full_name = user.full_name.strip()
    if full_name and full_name != str(user.tg_user_id):
        parts.append(full_name)
    return " / ".join(parts)


def _my_test_users_queryset():
    allowlisted_ids = list(getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []))
    if not allowlisted_ids:
        return TelegramUser.objects.filter(admin_state__is_test_user=True).select_related("admin_state").order_by("tg_user_id")
    return (
        TelegramUser.objects.filter(Q(tg_user_id__in=allowlisted_ids) | Q(admin_state__is_test_user=True))
        .select_related("admin_state")
        .order_by("tg_user_id")
        .distinct()
    )


def _resolve_user_lookup(raw_lookup: str, *, queryset=None) -> tuple[TelegramUser | None, list[TelegramUser], str | None]:
    lookup = (raw_lookup or "").strip()
    if not lookup:
        return None, [], None

    if queryset is None:
        queryset = TelegramUser.objects.select_related("admin_state")
    if lookup.isdigit():
        user = queryset.filter(tg_user_id=int(lookup)).first()
        if user:
            return user, [user], None

    username = lookup.lstrip("@")
    exact_username = queryset.filter(username__iexact=username).first()
    if exact_username:
        return exact_username, [exact_username], None

    candidates = list(
        queryset.filter(
            Q(username__icontains=username)
            | Q(first_name__icontains=lookup)
            | Q(last_name__icontains=lookup)
        )
        .order_by("-last_seen_at", "-created_at")[:20]
    )
    if not candidates:
        return None, [], "Користувача не знайдено. Використайте Telegram ID, username або ім'я."
    if len(candidates) == 1:
        return candidates[0], candidates, None
    return None, candidates, "Знайдено кілька користувачів. Уточніть Telegram ID або username."


def _remember_manual_message_test(request, *, user: TelegramUser, message_text: str, parse_mode: str) -> None:
    request.session[MANUAL_MESSAGE_TEST_SESSION_KEY] = {
        "telegram_user_id": user.tg_user_id,
        "message_text": message_text,
        "parse_mode": parse_mode,
        "admin_user_id": request.user.pk,
        "created_at": timezone.now().isoformat(),
    }
    request.session.modified = True


def _manual_message_test_matches(request, *, user: TelegramUser, message_text: str, parse_mode: str) -> bool:
    payload = request.session.get(MANUAL_MESSAGE_TEST_SESSION_KEY)
    if not payload:
        return False
    try:
        created_at = datetime.fromisoformat(payload["created_at"])
    except Exception:
        return False
    if timezone.is_naive(created_at):
        created_at = timezone.make_aware(created_at, timezone.get_current_timezone())
    if timezone.now() - created_at > MANUAL_MESSAGE_TEST_TTL:
        return False
    return (
        payload.get("telegram_user_id") == user.tg_user_id
        and payload.get("message_text") == message_text
        and payload.get("parse_mode") == parse_mode
        and payload.get("admin_user_id") == request.user.pk
    )


def _build_debug_context(user: TelegramUser, *, request, reason: str) -> dict:
    _require_test_tools(request)
    reason = str(reason or "").strip()
    if request.method != "POST" or not reason or len(reason) > 1000:
        raise PermissionDenied("Для QA-діагностики потрібна причина та підтверджений POST-запит.")
    is_test = UserAdminState.objects.filter(telegram_user_id=user.tg_user_id).values_list("is_test_user", flat=True).first()
    if not is_test and user.tg_user_id not in getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []):
        raise PermissionDenied("Діагностика доступна лише для тестових користувачів.")
    # Persist the reason-bound read audit before loading any raw input. Audit
    # failure must not turn into an unaudited diagnostic response.
    create_audit_log(
        request=request, admin_user=request.user, action="onboarding_debug_viewed",
        object_type="telegram_user", object_id=user.pk, target_user_id=user.tg_user_id,
        reason=reason, mode="qa_test_target", before={},
        after={"scope": "onboarding_debug", "raw_input_requested": True},
    )
    state = UserAdminState.objects.filter(telegram_user_id=user.tg_user_id).first()
    onboarding_events = list(BotEvent.objects.filter(user_id=user.tg_user_id, event_type__in=ONBOARDING_EVENT_TYPES).order_by("-created_at")[:20])
    input_events = list(BotEvent.objects.filter(user_id=user.tg_user_id, event_type__in=["text_received", "voice_received"]).order_by("-created_at")[:10])
    parse_errors = list(BotEvent.objects.filter(user_id=user.tg_user_id, event_type="parse_error").order_by("-created_at")[:10])
    bot_errors = list(BotEvent.objects.filter(user_id=user.tg_user_id, success=False).order_by("-created_at")[:10])
    onboarding_status = "Завершено" if user.onboarding_completed else "Не завершено"
    current_step = (state.current_fsm_state if state else "") or "Невідомо"
    report_lines = [
        f"ID користувача: {user.tg_user_id}",
        f"Telegram ID: {user.tg_user_id}",
        f"Username: @{user.username}" if user.username else "Username: -",
        f"Статус онбордингу: {onboarding_status}",
        f"Поточний крок: {current_step}",
        f"FSM стан: {current_step}",
        f"Час формування: {timezone.localtime().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Останні події:",
    ]
    if onboarding_events:
        report_lines.extend(
            [
                f"- {event.created_at:%Y-%m-%d %H:%M:%S} | {event.event_type} | {event.source or '-'} | {event.error_message or '-'}"
                for event in onboarding_events
            ]
        )
    else:
        report_lines.append("- немає")
    report_lines.extend(["", "Останні помилки:"])
    if bot_errors:
        report_lines.extend(
            [f"- {event.created_at:%Y-%m-%d %H:%M:%S} | {event.event_type} | {event.error_message or '-'}" for event in bot_errors]
        )
    else:
        report_lines.append("- немає")

    return {
        "user": user,
        "state": state,
        "onboarding_status": onboarding_status,
        "current_step": current_step,
        "redis_state": "Окреме читання Redis/FSM у цій реалізації не підключене.",
        "onboarding_events": onboarding_events,
        "input_events": input_events,
        "parse_errors": parse_errors,
        "bot_errors": bot_errors,
        "report_text": "\n".join(report_lines),
    }


class ResetMyOnboardingForm(forms.Form):
    user_id = forms.ChoiceField(label="Користувач")
    mode = forms.ChoiceField(choices=RESET_MODES, initial="safe", label="Режим скидання")
    reason = forms.CharField(
        widget=forms.Textarea(attrs={"rows": 4}),
        initial="QA / повторний прохід онбордингу",
        label="Причина",
    )
    clear_fsm_state = forms.BooleanField(required=False, initial=True, label="Очистити FSM / тимчасовий стан")
    send_telegram_notice = forms.BooleanField(required=False, initial=True, label="Надіслати підказку в Telegram")
    send_admin_notification = forms.BooleanField(required=False, initial=True, label="Надіслати службовий пуш адміну")
    double_confirmed = forms.BooleanField(required=False, initial=False, label="Підтверджую небезпечну дію")
    confirmation_text = forms.CharField(required=False, label='Підтвердження для повного скидання: введіть "RESET USER"')

    def __init__(self, *args, users_queryset=None, initial_user_id=None, **kwargs):
        super().__init__(*args, **kwargs)
        users_queryset = users_queryset or TelegramUser.objects.none()
        self.fields["user_id"].choices = [(str(user.tg_user_id), _user_option_label(user)) for user in users_queryset]
        if initial_user_id is not None:
            self.initial.setdefault("user_id", str(initial_user_id))


class TestMessageForm(forms.Form):
    user_id = forms.ChoiceField(label="Користувач")
    message_text = forms.CharField(
        widget=forms.Textarea(attrs={"rows": 4}),
        initial="Тестове повідомлення з адмінки Cash Flow Bot.",
        label="Текст повідомлення",
    )
    parse_mode = forms.ChoiceField(choices=PARSE_MODE_CHOICES, initial=Broadcast.ParseMode.NONE, label="Формат")

    def __init__(self, *args, users_queryset=None, **kwargs):
        super().__init__(*args, **kwargs)
        users_queryset = users_queryset or TelegramUser.objects.none()
        self.fields["user_id"].choices = [(str(user.tg_user_id), _user_option_label(user)) for user in users_queryset]


class BroadcastDryRunForm(forms.Form):
    segment = forms.ModelChoiceField(queryset=Segment.objects.none(), label="Сегмент")
    message_text = forms.CharField(
        widget=forms.Textarea(attrs={"rows": 3}),
        required=False,
        initial="Лише прев'ю для сухого прогону.",
        label="Текст прев'ю",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["segment"].queryset = (
            Segment.objects.filter(is_active=True, is_system=True)
            .exclude(slug__in=["users_by_tag", "users_by_source"])
            .order_by("name")
        )


class TestBroadcastForm(forms.Form):
    title = forms.CharField(initial="Тестова розсилка QA", label="Назва розсилки")
    message_text = forms.CharField(
        widget=forms.Textarea(attrs={"rows": 4}),
        initial="Тестова розсилка тільки для тестових користувачів.",
        label="Текст повідомлення",
    )
    parse_mode = forms.ChoiceField(choices=PARSE_MODE_CHOICES, initial=Broadcast.ParseMode.NONE, label="Формат")


class ManualMessagePageForm(forms.Form):
    user_lookup = forms.CharField(
        label="Користувач",
        help_text="Введіть Telegram ID, username або ім'я користувача.",
    )
    message_text = forms.CharField(widget=forms.Textarea(attrs={"rows": 8}), max_length=4000, label="Текст повідомлення")
    parse_mode = forms.ChoiceField(choices=PARSE_MODE_CHOICES, initial=Broadcast.ParseMode.NONE, label="Формат")


class UserLookupForm(forms.Form):
    user_lookup = forms.CharField(label="Тестовий користувач", help_text="Telegram ID, username або ім'я.")
    reason = forms.CharField(label="Причина QA-діагностики", max_length=1000,
                             help_text="Не додавайте фінансові дані. Причину й факт перегляду буде записано в аудит.")


def qa_tools_view(admin_site, request):
    _require_test_tools(request)
    if request.method == "POST" and request.POST.get("action") == "reset_my_onboarding" and request.POST.get("reset-mode", "safe") != "safe":
        require_admin_action(request.user, "test_cleanup")

    my_test_users = _my_test_users_queryset()
    reset_form = ResetMyOnboardingForm(request.POST or None, prefix="reset", users_queryset=my_test_users)
    message_form = TestMessageForm(request.POST or None, prefix="message", users_queryset=my_test_users)
    dry_run_form = BroadcastDryRunForm(request.POST or None, prefix="dryrun")
    broadcast_form = TestBroadcastForm(request.POST or None, prefix="broadcast")
    action_result = None
    health_result = run_admin_healthcheck(admin_user=request.user, record_event=False)

    if request.method == "POST":
        action = request.POST.get("action", "").strip()
        try:
            if action == "send_test_admin_notification":
                payload = {"admin_user_id": request.user.pk, "time": str(health_result["generated_at"])}
                send_admin_notification_task.delay(
                    "test_admin_notification_sent",
                    "Тестовий службовий пуш з адмінки Cash Flow Bot.",
                    payload,
                )
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="test_admin_notification_sent",
                    object_type="admin_notification",
                    object_id="test",
                    after=payload,
                )
                messages.success(request, "Тестовий пуш поставлено в чергу.")

            elif action == "send_test_message" and message_form.is_valid():
                user = TelegramUser.objects.get(pk=int(message_form.cleaned_data["user_id"]))
                log = AdminMessageLog.objects.create(
                    telegram_user_id=user.tg_user_id,
                    admin_user=request.user,
                    message_text=message_form.cleaned_data["message_text"],
                    parse_mode=message_form.cleaned_data["parse_mode"],
                    send_test_to_admin_first=False,
                    target_chat_id=user.tg_user_id,
                )
                send_manual_message_task.delay(log.id, is_test=True)
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="test_message_sent",
                    object_type="telegram_user",
                    object_id=user.pk,
                    target_user_id=user.tg_user_id,
                    after={"message_log_id": log.pk},
                )
                messages.success(request, "Тестове повідомлення поставлено в чергу.")

            elif action == "reset_my_onboarding" and reset_form.is_valid():
                result = reset_user_onboarding(
                    int(reset_form.cleaned_data["user_id"]),
                    reset_form.cleaned_data["mode"],
                    request.user,
                    reset_form.cleaned_data["reason"],
                    options={
                        "request": request,
                        "clear_fsm_state": reset_form.cleaned_data["clear_fsm_state"],
                        "send_telegram_notice": reset_form.cleaned_data["send_telegram_notice"],
                        "send_admin_notification": reset_form.cleaned_data["send_admin_notification"],
                        "double_confirmed": reset_form.cleaned_data["double_confirmed"],
                        "confirmation_text": reset_form.cleaned_data["confirmation_text"],
                    },
                )
                action_result = {"type": "reset", "payload": result}
                messages.success(request, "Онбординг скинуто. Тепер напишіть /start у Telegram.")

            elif action == "broadcast_dry_run" and dry_run_form.is_valid():
                broadcast = Broadcast.objects.create(
                    title=f"QA Сухий прогін: {dry_run_form.cleaned_data['segment'].name}",
                    message_text=dry_run_form.cleaned_data["message_text"] or "Лише прев'ю для сухого прогону.",
                    parse_mode=Broadcast.ParseMode.NONE,
                    created_by=request.user,
                    target_type=Broadcast.TargetType.SEGMENT,
                    target_segment=dry_run_form.cleaned_data["segment"],
                )
                send_broadcast_task(broadcast.id, dry_run=True)
                broadcast.refresh_from_db()
                action_result = {"type": "dry_run", "payload": broadcast.last_dry_run_preview}
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="broadcast_dry_run",
                    object_type="broadcast",
                    object_id=broadcast.pk,
                    mode="dry_run",
                    after=broadcast.last_dry_run_preview,
                )
                messages.success(request, "Сухий прогін завершено.")

            elif action == "send_test_broadcast" and broadcast_form.is_valid():
                segment = Segment.objects.filter(slug="test_users").first()
                if segment is None:
                    raise ResetOnboardingError("Сегмент test_users недоступний.")
                broadcast = Broadcast.objects.create(
                    title=broadcast_form.cleaned_data["title"],
                    message_text=broadcast_form.cleaned_data["message_text"],
                    parse_mode=broadcast_form.cleaned_data["parse_mode"],
                    created_by=request.user,
                    target_type=Broadcast.TargetType.SEGMENT,
                    target_segment=segment,
                    status=Broadcast.Status.SCHEDULED,
                )
                send_broadcast_task.delay(broadcast.id, dry_run=False)
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="test_broadcast_sent",
                    object_type="broadcast",
                    object_id=broadcast.pk,
                    mode="test_users",
                    after={"segment": "test_users"},
                )
                messages.success(request, "Тестову розсилку для тестових користувачів поставлено в чергу.")

            elif action == "run_healthcheck":
                health_result = run_admin_healthcheck(admin_user=request.user, record_event=True)
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="healthcheck_run",
                    object_type="system_health",
                    object_id="admin",
                    after={"summary_status": health_result["summary_status"]},
                )
                action_result = {"type": "healthcheck", "payload": health_result}
                messages.success(request, f"Перевірку завершено: {health_result['summary_status']}.")

        except ResetOnboardingError as exc:
            messages.error(request, str(exc))

    context = {
        **admin_site.each_context(request),
        "title": "Старі QA інструменти",
        "opts": TelegramUser._meta,
        "my_test_users": my_test_users,
        "reset_form": reset_form,
        "message_form": message_form,
        "dry_run_form": dry_run_form,
        "broadcast_form": broadcast_form,
        "health_result": health_result,
        "action_result": action_result,
        "admin_test_telegram_ids": getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []),
    }
    return TemplateResponse(request, "admin/qa_tools.html", context)


def manual_message_view(admin_site, request):
    require_admin_action(getattr(request, "user", None), "message_send")

    lookup_value = (request.POST.get("user_lookup") if request.method == "POST" else request.GET.get("user_lookup")) or ""
    form = ManualMessagePageForm(request.POST or None, initial={"user_lookup": lookup_value})
    resolved_user, matching_users, lookup_error = _resolve_user_lookup(lookup_value)
    latest_logs = (
        AdminMessageLog.objects.filter(telegram_user_id=resolved_user.tg_user_id).select_related("admin_user").order_by("-created_at")[:10]
        if resolved_user
        else []
    )

    if request.method == "POST" and form.is_valid():
        resolved_user, matching_users, lookup_error = _resolve_user_lookup(form.cleaned_data["user_lookup"])
        if lookup_error:
            messages.error(request, lookup_error)
        elif resolved_user is not None:
            action = request.POST.get("action", "").strip()
            message_text = form.cleaned_data["message_text"]
            parse_mode = form.cleaned_data["parse_mode"]

            if action == "send_test":
                log = AdminMessageLog.objects.create(
                    telegram_user_id=resolved_user.tg_user_id,
                    admin_user=request.user,
                    message_text=message_text,
                    parse_mode=parse_mode,
                    send_test_to_admin_first=True,
                    target_chat_id=resolved_user.tg_user_id,
                )
                send_manual_message_task.delay(log.id, is_test=True)
                _remember_manual_message_test(
                    request,
                    user=resolved_user,
                    message_text=message_text,
                    parse_mode=parse_mode,
                )
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="manual_message_test_sent",
                    object_type="telegram_user",
                    object_id=resolved_user.pk,
                    target_user_id=resolved_user.tg_user_id,
                    after={"message_log_id": log.pk},
                )
                messages.success(request, "Тестове повідомлення поставлено в чергу. Після цього можна надсилати користувачу.")

            elif action == "send_user":
                if not _manual_message_test_matches(
                    request,
                    user=resolved_user,
                    message_text=message_text,
                    parse_mode=parse_mode,
                ):
                    messages.error(request, "Спочатку надішліть тест собі з цим самим текстом і форматом.")
                elif not resolved_user.start_date:
                    log = AdminMessageLog.objects.create(
                        telegram_user_id=resolved_user.tg_user_id,
                        admin_user=request.user,
                        message_text=message_text,
                        parse_mode=parse_mode,
                        send_test_to_admin_first=False,
                        target_chat_id=resolved_user.tg_user_id,
                        status=AdminMessageLog.Status.SKIPPED,
                        error_message="Користувач ще не стартував бота.",
                    )
                    create_audit_log(
                        request=request,
                        admin_user=request.user,
                        action="manual_message_skipped",
                        object_type="telegram_user",
                        object_id=resolved_user.pk,
                        target_user_id=resolved_user.tg_user_id,
                        after={"message_log_id": log.pk, "status": AdminMessageLog.Status.SKIPPED},
                    )
                    messages.warning(request, "Користувач ще не стартував бота. Відправку пропущено.")
                else:
                    state = getattr(resolved_user, "admin_state", None)
                    if state and state.blocked_bot:
                        log = AdminMessageLog.objects.create(
                            telegram_user_id=resolved_user.tg_user_id,
                            admin_user=request.user,
                            message_text=message_text,
                            parse_mode=parse_mode,
                            send_test_to_admin_first=False,
                            target_chat_id=resolved_user.tg_user_id,
                            status=AdminMessageLog.Status.BLOCKED,
                            error_message="Користувач раніше заблокував бота.",
                        )
                        create_audit_log(
                            request=request,
                            admin_user=request.user,
                            action="manual_message_blocked",
                            object_type="telegram_user",
                            object_id=resolved_user.pk,
                            target_user_id=resolved_user.tg_user_id,
                            after={"message_log_id": log.pk, "status": AdminMessageLog.Status.BLOCKED},
                        )
                        messages.warning(request, "Користувач раніше заблокував бота. У логах зафіксовано статус 'Заблокував бота'.")
                    else:
                        log = AdminMessageLog.objects.create(
                            telegram_user_id=resolved_user.tg_user_id,
                            admin_user=request.user,
                            message_text=message_text,
                            parse_mode=parse_mode,
                            send_test_to_admin_first=False,
                            target_chat_id=resolved_user.tg_user_id,
                        )
                        send_manual_message_task.delay(log.id, is_test=False)
                        create_audit_log(
                            request=request,
                            admin_user=request.user,
                            action="manual_message_sent",
                            object_type="telegram_user",
                            object_id=resolved_user.pk,
                            target_user_id=resolved_user.tg_user_id,
                            after={"message_log_id": log.pk},
                        )
                        messages.success(request, "Повідомлення поставлено в чергу. Статус дивіться в логах нижче.")

            latest_logs = AdminMessageLog.objects.filter(telegram_user_id=resolved_user.tg_user_id).select_related("admin_user").order_by("-created_at")[:10]

    context = {
        **admin_site.each_context(request),
        "title": "Написати користувачу",
        "form": form,
        "resolved_user": resolved_user,
        "matching_users": matching_users if not resolved_user else [],
        "lookup_error": lookup_error,
        "latest_logs": latest_logs,
        "cancel_url": reverse("admin:index"),
    }
    return TemplateResponse(request, "admin/manual_message.html", context)


def my_test_user_view(admin_site, request):
    _require_test_tools(request)
    my_test_users = list(_my_test_users_queryset())
    for user in my_test_users:
        state, _ = UserAdminState.objects.get_or_create(telegram_user_id=user.tg_user_id)
        user.admin_state = state

    context = {
        **admin_site.each_context(request),
        "title": "Мій тестовий користувач",
        "my_test_users": my_test_users,
        "admin_test_telegram_ids": getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []),
        "open_user_url_name": "admin:users_telegramuser_change",
        "reset_url_name": "admin:reset_my_onboarding",
        "debug_url_name": "admin:onboarding_debug",
        "manual_message_url": reverse("admin:manual_message"),
    }
    return TemplateResponse(request, "admin/my_test_user.html", context)


def reset_my_onboarding_view(admin_site, request):
    _require_test_tools(request)
    if request.method == "POST" and request.POST.get("mode", "safe") != "safe":
        require_admin_action(request.user, "test_cleanup")

    my_test_users = _my_test_users_queryset()
    initial_user_id = request.GET.get("user_id") or request.GET.get("telegram_id")
    form = ResetMyOnboardingForm(
        request.POST or None,
        users_queryset=my_test_users,
        initial_user_id=initial_user_id,
    )
    action_result = None

    if request.method == "POST" and form.is_valid():
        try:
            result = reset_user_onboarding(
                int(form.cleaned_data["user_id"]),
                form.cleaned_data["mode"],
                request.user,
                form.cleaned_data["reason"],
                options={
                    "request": request,
                    "clear_fsm_state": form.cleaned_data["clear_fsm_state"],
                    "send_telegram_notice": form.cleaned_data["send_telegram_notice"],
                    "send_admin_notification": form.cleaned_data["send_admin_notification"],
                    "double_confirmed": form.cleaned_data["double_confirmed"],
                    "confirmation_text": form.cleaned_data["confirmation_text"],
                },
            )
            action_result = result
            messages.success(request, "Онбординг скинуто. Тепер напишіть /start у Telegram.")
        except ResetOnboardingError as exc:
            messages.error(request, str(exc))

    context = {
        **admin_site.each_context(request),
        "title": "Скинути мій онбординг",
        "form": form,
        "action_result": action_result,
        "admin_test_telegram_ids": getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []),
    }
    return TemplateResponse(request, "admin/reset_my_onboarding.html", context)


def onboarding_debug_view(admin_site, request):
    _require_test_tools(request)

    lookup_value = (request.POST.get("user_lookup") if request.method == "POST" else request.GET.get("user_lookup")) or ""
    if not lookup_value and request.GET.get("user_id"):
        lookup_value = request.GET["user_id"]
    form = UserLookupForm(request.POST or None, initial={"user_lookup": lookup_value})
    resolved_user, matching_users, lookup_error = None, [], None
    debug_context = None

    if request.method == "POST" and form.is_valid():
        # Lookup candidates are test targets only; never preload their raw state.
        queryset = _my_test_users_queryset().select_related(None).only(
            "tg_user_id", "username", "first_name", "last_name", "last_seen_at", "onboarding_completed",
        )
        resolved_user, matching_users, lookup_error = _resolve_user_lookup(form.cleaned_data["user_lookup"], queryset=queryset)
        if resolved_user:
            debug_context = _build_debug_context(resolved_user, request=request, reason=form.cleaned_data["reason"])
        if lookup_error:
            messages.error(request, lookup_error)

    context = {
        **admin_site.each_context(request),
        "title": "Debug онбордингу",
        "form": form,
        "resolved_user": resolved_user,
        "matching_users": matching_users if not resolved_user else [],
        "lookup_error": lookup_error,
        "debug": debug_context,
    }
    return TemplateResponse(request, "admin/onboarding_debug.html", context)


def admin_notifications_view(admin_site, request):
    require_admin_action(request.user, "notification_view")
    if request.method == "POST":
        require_admin_action(request.user, "notification_send")

    if request.method == "POST":
        payload = {"admin_user_id": request.user.pk, "time": timezone.localtime().isoformat()}
        send_admin_notification_task.delay(
            "test_admin_notification_sent",
            "Тестовий службовий пуш з адмінки Cash Flow Bot.",
            payload,
        )
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="test_admin_notification_sent",
            object_type="admin_notification",
            object_id="test",
            after=payload,
        )
        messages.success(request, "Тестовий пуш поставлено в чергу.")

    context = {
        **admin_site.each_context(request),
        "title": "Пуші адміну",
        "notifications_enabled": settings.ADMIN_NOTIFICATIONS_ENABLED,
        "admin_telegram_ids": settings.ADMIN_TELEGRAM_IDS,
        "telegram_api_check": _check_telegram_api(),
        "last_test_push": AdminNotificationLog.objects.filter(event_type="test_admin_notification_sent").order_by("-created_at").first(),
        "recent_logs": AdminNotificationLog.objects.order_by("-created_at")[:20],
        "event_labels": [
            "новий користувач",
            "завершив онбординг",
            "оплата",
            "помилка оплати",
            "критична помилка бота",
            "розсилка завершена",
            "розсилка впала",
            "скидання онбордингу",
            "3 parse errors від одного користувача",
        ],
    }
    return TemplateResponse(request, "admin/admin_notifications.html", context)


def healthcheck_view(admin_site, request):
    require_admin_action(request.user, "health_view")

    result = run_admin_healthcheck(admin_user=request.user, record_event=request.method == "POST")
    if request.method == "POST":
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="healthcheck_run",
            object_type="system_health",
            object_id="admin",
            after={"summary_status": result["summary_status"]},
        )
        messages.success(request, f"Перевірку завершено: {result['summary_status']}.")
    context = {
        **admin_site.each_context(request),
        "title": "Перевірка системи",
        "health_result": result,
    }
    return TemplateResponse(request, "admin/healthcheck.html", context)
