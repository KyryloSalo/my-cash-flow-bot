from __future__ import annotations

from django import forms
from django.conf import settings
from django.contrib import admin, messages
from django.shortcuts import get_object_or_404, redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils.html import format_html, format_html_join
from django.utils import timezone

from broadcasts.models import AdminMessageLog, Broadcast, BroadcastRecipient, Segment
from broadcasts.targets import users_for_target
from broadcasts.tasks import send_broadcast_task, send_manual_message_task
from broadcasts.wizard import (
    BROADCAST_TEST_SESSION_KEY,
    GROUP_LABELS,
    LARGE_BROADCAST_CONFIRM_THRESHOLD,
    WIZARD_STEPS,
    BroadcastWizardLaunchForm,
    BroadcastWizardMessageForm,
    BroadcastWizardRecipientsForm,
    apply_recipients_form,
    broadcast_config_fingerprint,
    broadcast_test_matches,
    delivery_preview_rows,
    ensure_wizard_segments,
    is_editable_broadcast,
    recipient_group_from_broadcast,
    recipient_preview_data,
    recipients_form_initial,
    remember_broadcast_test,
)
from common.admin import AuditedModelAdmin, HiddenFromMenuAdminMixin
from common.admin_actions import admin_action_permission, require_admin_action
from common.admin_site import admin_site
from common.admin_ui import compact_text, status_badge, user_identity
from common.audit import create_audit_log
from common.telegram_admins import get_primary_admin_telegram_id

Broadcast._meta.verbose_name = "Розсилка"
Broadcast._meta.verbose_name_plural = "Розсилки"
AdminMessageLog._meta.verbose_name = "Лог повідомлень"
AdminMessageLog._meta.verbose_name_plural = "Логи повідомлень"
BroadcastRecipient._meta.verbose_name = "Доставка розсилки"
BroadcastRecipient._meta.verbose_name_plural = "Логи доставки розсилок"
Segment._meta.verbose_name = "Сегмент"
Segment._meta.verbose_name_plural = "Сегменти"


BROADCAST_STATUS_LABELS = {
    Broadcast.Status.DRAFT: "Чернетка",
    Broadcast.Status.SCHEDULED: "Запланована",
    Broadcast.Status.SENDING: "Відправляється",
    Broadcast.Status.COMPLETED: "Завершена",
    Broadcast.Status.SENT: "Завершена",
    Broadcast.Status.FAILED: "Помилка",
    Broadcast.Status.CANCELLED: "Скасована",
}
BROADCAST_TARGET_LABELS = {
    Broadcast.TargetType.ALL: "Усі активні користувачі",
    Broadcast.TargetType.SEGMENT: "Сегмент",
    Broadcast.TargetType.TAG: "Користувачі з тегом",
    Broadcast.TargetType.MANUAL_USERS: "Обрані вручну",
    Broadcast.TargetType.PUSH_TOPIC: "Тема повідомлень",
}
RECIPIENT_STATUS_LABELS = {
    BroadcastRecipient.Status.PENDING: "Черга",
    BroadcastRecipient.Status.UNCERTAIN: "У роботі / результат невідомий (без автоматичного повтору)",
    BroadcastRecipient.Status.SENT: "Надіслано",
    BroadcastRecipient.Status.FAILED: "Помилка",
    BroadcastRecipient.Status.BLOCKED: "Заблокував бота",
    BroadcastRecipient.Status.SKIPPED: "Пропущено",
}
MESSAGE_STATUS_LABELS = {
    AdminMessageLog.Status.QUEUED: "Черга",
    AdminMessageLog.Status.UNCERTAIN: "У роботі / результат невідомий (без автоматичного повтору)",
    AdminMessageLog.Status.SENT: "Надіслано",
    AdminMessageLog.Status.FAILED: "Помилка",
    AdminMessageLog.Status.BLOCKED: "Заблокував бота",
    AdminMessageLog.Status.SKIPPED: "Пропущено",
}


def _preview_table(headers: tuple[str, ...], rows):
    return format_html(
        "<table><thead><tr>{}</tr></thead><tbody>{}</tbody></table>",
        format_html_join("", "<th>{}</th>", ((header,) for header in headers)),
        format_html_join(
            "",
            "<tr>{}</tr>",
            (
                (format_html_join("", "<td>{}</td>", ((cell,) for cell in row)),)
                for row in rows
            ),
        ),
    )


class BroadcastAdminForm(forms.ModelForm):
    parse_mode = forms.ChoiceField(
        choices=[
            (Broadcast.ParseMode.NONE, "Звичайний текст"),
            (Broadcast.ParseMode.HTML, "HTML"),
            (Broadcast.ParseMode.MARKDOWN, "Markdown"),
        ],
        label="Формат",
    )
    target_type = forms.ChoiceField(
        choices=[
            (Broadcast.TargetType.ALL, "Усі активні користувачі"),
            (Broadcast.TargetType.SEGMENT, "Сегмент"),
            (Broadcast.TargetType.TAG, "Користувачі з тегом"),
            (Broadcast.TargetType.MANUAL_USERS, "Обрані вручну"),
            (Broadcast.TargetType.PUSH_TOPIC, "Тема повідомлень"),
        ],
        label="Отримувачі",
    )

    class Meta:
        model = Broadcast
        fields = (
            "title",
            "message_text",
            "button_text",
            "button_url",
            "parse_mode",
            "target_type",
            "target_segment",
            "target_tag",
            "target_topic",
            "manual_users",
        )
        labels = {
            "title": "Назва розсилки",
            "message_text": "Текст повідомлення",
            "button_text": "Текст кнопки",
            "button_url": "URL кнопки",
            "target_segment": "Сегмент",
            "target_tag": "Тег",
            "target_topic": "Тема повідомлень",
            "manual_users": "Telegram ID користувачів",
        }
        help_texts = {
            "manual_users": "Вкажіть Telegram ID через кому або з нового рядка.",
        }


class BroadcastSendConfirmForm(forms.Form):
    confirmation_text = forms.CharField(required=False, label='Для великої розсилки введіть "SEND"')


@admin.register(Segment, site=admin_site)
class SegmentAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "segment"
    list_display = ("name", "slug", "type", "is_active", "is_system", "updated_at")
    search_fields = ("name", "slug", "description")
    list_filter = ("type", "is_active", "is_system")
    readonly_fields = ("created_at", "updated_at")


@admin.register(Broadcast, site=admin_site)
class BroadcastAdmin(AuditedModelAdmin):
    audit_object_type = "broadcast"
    form = BroadcastAdminForm
    list_display = ("title", "status_label", "target_type_label", "total_recipients", "sent_count", "failed_count", "blocked_count", "uncertain_count", "created_at", "action_links")
    search_fields = ("title", "message_text")
    list_filter = ("status", "target_type", "parse_mode", "created_at", "started_at", "finished_at")
    readonly_fields = (
        "status_label",
        "recipient_preview",
        "last_dry_run_preview_pretty",
        "delivery_log_preview",
        "total_recipients",
        "sent_count",
        "failed_count",
        "blocked_count",
        "skipped_count",
        "uncertain_count",
        "last_error",
        "started_at",
        "finished_at",
        "created_at",
        "updated_at",
        "action_links",
    )
    fieldsets = (
        ("Крок 1. Повідомлення", {"fields": ("title", "message_text", "parse_mode", "button_text", "button_url")}),
        ("Крок 2. Отримувачі", {"fields": ("target_type", "target_segment", "target_tag", "target_topic", "manual_users", "recipient_preview")}),
        ("Крок 3. Перевірка", {"fields": ("last_dry_run_preview_pretty", "action_links")}),
        ("Крок 4. Відправка", {"fields": ("status_label", "total_recipients", "sent_count", "failed_count", "blocked_count", "skipped_count", "uncertain_count", "last_error", "delivery_log_preview")}),
    )

    def has_delete_permission(self, request, obj=None):
        return False

    def get_fieldsets(self, request, obj=None):
        fieldsets = list(self.fieldsets)
        if settings.ENABLE_ADMIN_DEVELOPER_MODE and getattr(request.user, "is_superuser", False):
            fieldsets.append(
                (
                    "Технічна інформація",
                    {
                        "fields": (
                            "image_url",
                            "segment_filter",
                            "scheduled_at",
                            "started_at",
                            "finished_at",
                            "created_at",
                            "updated_at",
                        )
                    },
                )
            )
        return fieldsets

    @admin_action_permission("broadcast_create")
    def add_view(self, request, form_url="", extra_context=None):
        return redirect("admin:broadcasts_broadcast_wizard_new")

    @admin_action_permission("broadcast_view")
    def change_view(self, request, object_id, form_url="", extra_context=None):
        return redirect("admin:broadcasts_broadcast_wizard_step", object_id=object_id, step=1)

    def _wizard_step_url(self, broadcast: Broadcast, step: int) -> str:
        return reverse("admin:broadcasts_broadcast_wizard_step", args=[broadcast.pk, step])

    def _wizard_steps(self, broadcast: Broadcast | None, current_step: int) -> list[dict]:
        items = []
        for step_number, label in WIZARD_STEPS:
            if broadcast is None:
                url = reverse("admin:broadcasts_broadcast_wizard_new") if step_number == 1 else None
            else:
                url = self._wizard_step_url(broadcast, step_number)
            items.append(
                {
                    "number": step_number,
                    "label": label,
                    "url": url,
                    "is_current": step_number == current_step,
                    "is_available": url is not None,
                }
            )
        return items

    def _resolve_admin_test_chat_id(self, request) -> int | None:
        if hasattr(request.user, "profile") and getattr(request.user.profile, "telegram_chat_id", None):
            return request.user.profile.telegram_chat_id
        return get_primary_admin_telegram_id()

    @admin_action_permission("broadcast_change")
    def _schedule_broadcast(self, request, broadcast: Broadcast) -> bool:
        if not is_editable_broadcast(broadcast):
            return False
        # A stale second confirmation must not reset a sending/finished job.
        claimed = Broadcast.objects.filter(pk=broadcast.pk, status=broadcast.status).update(
            status=Broadcast.Status.SCHEDULED, updated_at=timezone.now(),
        )
        if not claimed:
            messages.warning(request, "Стан розсилки змінився. Оновіть сторінку; повторну задачу не створено.")
            return False
        send_broadcast_task.delay(broadcast.id, dry_run=False)
        return True

    def _broadcast_test_ready(self, request, broadcast: Broadcast) -> bool:
        # Session approval binds content + exact audience; the worker receipt,
        # not queue acceptance, proves that this administrator's test succeeded.
        if not broadcast_test_matches(request, broadcast):
            return False
        receipt = request.session.get(BROADCAST_TEST_SESSION_KEY, {})
        log_id = receipt.get("message_log_id")
        if not isinstance(log_id, int) or isinstance(log_id, bool):
            return False
        return AdminMessageLog.objects.filter(
            pk=log_id, admin_user=request.user, telegram_user__isnull=True,
            send_test_to_admin_first=True, target_chat_id=receipt.get("target_chat_id"),
            status=AdminMessageLog.Status.SENT, sent_at__isnull=False,
            message_text=broadcast.message_text, parse_mode=broadcast.parse_mode,
            image_url=broadcast.image_url,
            buttons_payload=[[{"text": broadcast.button_text, "url": broadcast.button_url}]] if broadcast.button_text and broadcast.button_url else [],
        ).exists()

    @admin_action_permission("broadcast_change")
    def _queue_broadcast_test_message(self, request, broadcast: Broadcast) -> bool:
        telegram_chat_id = self._resolve_admin_test_chat_id(request)
        if telegram_chat_id is None:
            messages.error(request, "ADMIN_TELEGRAM_IDS не налаштовано.")
            return False

        log = AdminMessageLog.objects.create(
            telegram_user=None,
            admin_user=request.user,
            message_text=broadcast.message_text,
            parse_mode=broadcast.parse_mode,
            send_test_to_admin_first=True,
            target_chat_id=telegram_chat_id,
            buttons_payload=[[{"text": broadcast.button_text, "url": broadcast.button_url}]] if broadcast.button_text and broadcast.button_url else [],
            image_url=broadcast.image_url,
        )
        send_manual_message_task.delay(log.id, is_test=False)
        remember_broadcast_test(request, broadcast)
        request.session[BROADCAST_TEST_SESSION_KEY].update(
            message_log_id=log.pk, target_chat_id=telegram_chat_id,
        )
        request.session.modified = True
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="broadcast_test_queued",
            object_type="broadcast",
            object_id=broadcast.pk,
            after={"message_log_id": log.pk},
        )
        messages.success(request, "Тестове повідомлення поставлено в чергу адміну.")
        return True

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("wizard/new/", self.admin_site.admin_view(self.wizard_new_view), name="broadcasts_broadcast_wizard_new"),
            path("<path:object_id>/wizard/<int:step>/", self.admin_site.admin_view(self.wizard_view), name="broadcasts_broadcast_wizard_step"),
            path("<path:object_id>/preview/", self.admin_site.admin_view(self.preview_view), name="broadcasts_broadcast_preview"),
            path("<path:object_id>/send-test/", self.admin_site.admin_view(self.send_test_view), name="broadcasts_broadcast_send_test"),
            path("<path:object_id>/send/", self.admin_site.admin_view(self.send_view), name="broadcasts_broadcast_send"),
            path("<path:object_id>/cancel/", self.admin_site.admin_view(self.cancel_view), name="broadcasts_broadcast_cancel"),
        ]
        return custom_urls + urls

    @admin_action_permission("broadcast_create")
    def wizard_new_view(self, request):
        return self.wizard_view(request, step=1)

    def wizard_view(self, request, object_id=None, step=1):
        action = "broadcast_create" if object_id is None else "broadcast_change"
        require_admin_action(getattr(request, "user", None), action)
        ensure_wizard_segments()

        broadcast = get_object_or_404(Broadcast, pk=object_id) if object_id is not None else None
        current_step = max(1, min(int(step), 4))
        if broadcast is None and current_step != 1:
            return redirect("admin:broadcasts_broadcast_wizard_new")
        if broadcast and current_step < 4 and not is_editable_broadcast(broadcast):
            messages.warning(request, "Розсилку вже запущено або завершено. Доступний лише фінальний екран з логами.")
            return redirect(self._wizard_step_url(broadcast, 4))

        step_title = dict(WIZARD_STEPS)[current_step]
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "title": "Майстер розсилки",
            "wizard_title": step_title,
            "current_step": current_step,
            "steps": self._wizard_steps(broadcast, current_step),
            "broadcast": broadcast,
            "status_label": self.status_label(broadcast) if broadcast else "Чернетка",
            "list_url": reverse("admin:broadcasts_broadcast_changelist"),
        }

        if current_step == 1:
            before = broadcast_config_fingerprint(broadcast)
            form = BroadcastWizardMessageForm(request.POST or None, instance=broadcast)
            if request.method == "POST" and form.is_valid():
                draft = form.save(commit=False)
                is_new = draft.pk is None
                if not draft.created_by_id:
                    draft.created_by = request.user
                if not draft.target_type:
                    draft.target_type = Broadcast.TargetType.ALL
                draft.status = draft.status or Broadcast.Status.DRAFT
                after = broadcast_config_fingerprint(draft)
                if before != after:
                    draft.last_dry_run_preview = {}
                draft.save()
                if is_new:
                    create_audit_log(
                        request=request,
                        admin_user=request.user,
                        action="broadcast created",
                        object_type="broadcast",
                        object_id=draft.pk,
                        before={},
                        after={"title": draft.title},
                    )
                elif before != after:
                    create_audit_log(
                        request=request,
                        admin_user=request.user,
                        action="broadcast_updated",
                        object_type="broadcast",
                        object_id=draft.pk,
                        mode="wizard_step_1",
                        before={"fingerprint": list(before)},
                        after={"fingerprint": list(after)},
                    )
                messages.success(request, "Крок 1 збережено.")
                if request.POST.get("action") == "stay":
                    return redirect(self._wizard_step_url(draft, 1))
                return redirect(self._wizard_step_url(draft, 2))
            context["form"] = form
            return TemplateResponse(request, "admin/broadcast_wizard.html", context)

        if broadcast is None:
            return redirect("admin:broadcasts_broadcast_wizard_new")

        if current_step == 2:
            before = broadcast_config_fingerprint(broadcast)
            form = BroadcastWizardRecipientsForm(request.POST or None, initial=recipients_form_initial(broadcast))
            if request.method == "POST" and form.is_valid():
                apply_recipients_form(broadcast, form.cleaned_data)
                after = broadcast_config_fingerprint(broadcast)
                if before != after:
                    broadcast.last_dry_run_preview = {}
                broadcast.save()
                if before != after:
                    create_audit_log(
                        request=request,
                        admin_user=request.user,
                        action="broadcast_updated",
                        object_type="broadcast",
                        object_id=broadcast.pk,
                        mode="wizard_step_2",
                        before={"fingerprint": list(before)},
                        after={"fingerprint": list(after)},
                    )
                action = request.POST.get("action", "next")
                if action == "back":
                    return redirect(self._wizard_step_url(broadcast, 1))
                if action == "preview":
                    messages.success(request, "Крок 2 збережено. Прев'ю отримувачів оновлено.")
                    return redirect(self._wizard_step_url(broadcast, 2))
                messages.success(request, "Крок 2 збережено.")
                return redirect(self._wizard_step_url(broadcast, 3))
            context["form"] = form
            context["recipient_preview"] = recipient_preview_data(broadcast)
            context["recipient_group_label"] = GROUP_LABELS.get(recipient_group_from_broadcast(broadcast), "Отримувачі")
            return TemplateResponse(request, "admin/broadcast_wizard.html", context)

        if current_step == 3:
            if request.method == "POST":
                action = request.POST.get("action", "").strip()
                if action == "back":
                    return redirect(self._wizard_step_url(broadcast, 2))
                if action == "dry_run":
                    send_broadcast_task(broadcast.id, dry_run=True)
                    broadcast.refresh_from_db()
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
                    return redirect(self._wizard_step_url(broadcast, 3))
                if action == "send_test":
                    self._queue_broadcast_test_message(request, broadcast)
                    return redirect(self._wizard_step_url(broadcast, 3))
                if action == "next":
                    preview = recipient_preview_data(broadcast)
                    if preview["count"] <= 0:
                        messages.error(request, "Для цієї розсилки не знайдено жодного отримувача.")
                        return redirect(self._wizard_step_url(broadcast, 2))
                    if not broadcast.last_dry_run_preview:
                        messages.error(request, "Спочатку виконайте сухий прогін.")
                        return redirect(self._wizard_step_url(broadcast, 3))
                    if not self._broadcast_test_ready(request, broadcast):
                        messages.error(request, "Потрібна підтверджена доставка тесту адміну для поточної версії розсилки. Перевірте лог повідомлення; черга або невідомий результат не є підтвердженням.")
                        return redirect(self._wizard_step_url(broadcast, 3))
                    return redirect(self._wizard_step_url(broadcast, 4))
            context["recipient_preview"] = recipient_preview_data(broadcast)
            context["recipient_group_label"] = GROUP_LABELS.get(recipient_group_from_broadcast(broadcast), "Отримувачі")
            context["dry_run_preview"] = broadcast.last_dry_run_preview
            context["test_ready"] = self._broadcast_test_ready(request, broadcast)
            return TemplateResponse(request, "admin/broadcast_wizard.html", context)

        launch_form = BroadcastWizardLaunchForm(request.POST or None)
        preview = recipient_preview_data(broadcast)
        can_launch = is_editable_broadcast(broadcast) and preview["count"] > 0
        can_cancel = broadcast.status in {
            Broadcast.Status.DRAFT,
            Broadcast.Status.FAILED,
            Broadcast.Status.CANCELLED,
            Broadcast.Status.SCHEDULED,
            Broadcast.Status.SENDING,
        }
        if request.method == "POST":
            action = request.POST.get("action", "").strip()
            if action == "back":
                return redirect(self._wizard_step_url(broadcast, 3))
            if action == "send":
                if not broadcast.last_dry_run_preview:
                    messages.error(request, "Спочатку виконайте сухий прогін.")
                    return redirect(self._wizard_step_url(broadcast, 3))
                if not self._broadcast_test_ready(request, broadcast):
                    messages.error(request, "Потрібна підтверджена доставка тесту адміну для поточної версії розсилки. Перевірте лог повідомлення; черга або невідомий результат не є підтвердженням.")
                    return redirect(self._wizard_step_url(broadcast, 3))
                if not can_launch:
                    messages.error(request, "Для цієї розсилки зараз немає валідних отримувачів.")
                    return redirect(self._wizard_step_url(broadcast, 2))
                if launch_form.is_valid() and preview["count"] >= LARGE_BROADCAST_CONFIRM_THRESHOLD and launch_form.cleaned_data["confirmation_text"].strip() != "SEND":
                    messages.error(request, 'Для великої розсилки введіть "SEND".')
                    return redirect(self._wizard_step_url(broadcast, 4))
                before = {"status": broadcast.status}
                if not self._schedule_broadcast(request, broadcast):
                    return redirect(self._wizard_step_url(broadcast, 4))
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="broadcast sent",
                    object_type="broadcast",
                    object_id=broadcast.pk,
                    before=before,
                    after={"status": Broadcast.Status.SCHEDULED},
                )
                messages.success(request, "Розсилку поставлено в чергу.")
                return redirect(self._wizard_step_url(broadcast, 4))
        context["form"] = launch_form
        context["recipient_preview"] = preview
        context["recipient_group_label"] = GROUP_LABELS.get(recipient_group_from_broadcast(broadcast), "Отримувачі")
        context["delivery_rows"] = [
            {"user_id": row.user_id, "status": RECIPIENT_STATUS_LABELS.get(row.status, row.status),
             "sent_at": row.sent_at, "error_message": row.error_message}
            for row in delivery_preview_rows(broadcast)
        ]
        context["can_launch"] = can_launch
        context["can_cancel"] = can_cancel
        context["test_ready"] = self._broadcast_test_ready(request, broadcast)
        context["cancel_url"] = reverse("admin:broadcasts_broadcast_cancel", args=[broadcast.pk])
        context["large_broadcast_threshold"] = LARGE_BROADCAST_CONFIRM_THRESHOLD
        return TemplateResponse(request, "admin/broadcast_wizard.html", context)

    @admin.display(description="Статус")
    def status_label(self, obj: Broadcast):
        label = BROADCAST_STATUS_LABELS.get(obj.status, obj.status)
        if obj.uncertain_count:
            return f"{label} · у роботі / невідомо: {obj.uncertain_count} (без автоматичного повтору)"
        return label

    @admin.display(description="Отримувачі")
    def target_type_label(self, obj: Broadcast):
        if obj.target_type == Broadcast.TargetType.SEGMENT and obj.target_segment:
            group = GROUP_LABELS.get(recipient_group_from_broadcast(obj))
            if group:
                return group
        if obj.target_type == Broadcast.TargetType.TAG:
            return GROUP_LABELS["tag"]
        if obj.target_type == Broadcast.TargetType.MANUAL_USERS:
            return GROUP_LABELS["manual"]
        if obj.target_type == Broadcast.TargetType.PUSH_TOPIC:
            return GROUP_LABELS["topic"]
        return BROADCAST_TARGET_LABELS.get(obj.target_type, obj.target_type)

    @admin.display(description="Прев'ю отримувачів")
    def recipient_preview(self, obj: Broadcast):
        if not obj.pk:
            return "Збережіть чернетку, щоб побачити підрахунок отримувачів."
        preview = recipient_preview_data(obj)
        if not preview["users"]:
            return "Отримувачів поки не визначено."
        items = format_html_join(
            "",
            "<div>{} / @{} / {}</div>",
            ((user.tg_user_id, user.username or "-", user.full_name) for user in preview["users"]),
        )
        return format_html(
            "<div><strong>Буде отримувачів:</strong> {}</div><div style='margin-top:8px;'>{}</div>",
            preview["count"],
            items,
        )

    @admin.display(description="Сухий прогін / прев'ю")
    def last_dry_run_preview_pretty(self, obj: Broadcast):
        if not obj.pk:
            return "Збережіть чернетку, потім виконайте сухий прогін."
        preview = obj.last_dry_run_preview or {}
        if not preview:
            return "Спочатку запустіть сухий прогін або тестове надсилання."
        return format_html("<pre>{}</pre>", preview)

    @admin.display(description="Логи доставки")
    def delivery_log_preview(self, obj: Broadcast):
        if not obj.pk:
            return "Логи доставки з'являться після запуску розсилки."
        rows = delivery_preview_rows(obj)
        if not rows:
            return "Логів доставки поки немає."
        return _preview_table(
            ("Користувач", "Статус", "Дата", "Помилка"),
            (
                (
                    row.user_id,
                    RECIPIENT_STATUS_LABELS.get(row.status, row.status),
                    row.sent_at or "-",
                    row.error_message or "-",
                )
                for row in rows
            ),
        )

    @admin.display(description="Дії")
    def action_links(self, obj: Broadcast):
        if not obj.pk:
            return "Створення нової розсилки відбувається через покроковий майстер."
        return format_html(
            '<div style="display:flex; gap:8px; flex-wrap:wrap;">'
            '<a class="button" href="{}">Відкрити майстер</a>'
            '<a class="button" href="{}">Сухий прогін</a>'
            '<a class="button" href="{}">Надіслати тест мені</a>'
            '<a class="button" href="{}">Запустити розсилку</a>'
            '<a class="button" href="{}">Скасувати</a>'
            "</div>",
            self._wizard_step_url(obj, 1),
            reverse("admin:broadcasts_broadcast_preview", args=[obj.pk]),
            reverse("admin:broadcasts_broadcast_send_test", args=[obj.pk]),
            reverse("admin:broadcasts_broadcast_send", args=[obj.pk]),
            reverse("admin:broadcasts_broadcast_cancel", args=[obj.pk]),
        )

    def save_model(self, request, obj, form, change):
        if not obj.created_by_id:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)
        if not change:
            create_audit_log(
                request=request,
                admin_user=request.user,
                action="broadcast created",
                object_type="broadcast",
                object_id=obj.pk,
                before={},
                after={"title": obj.title},
            )

    @admin_action_permission("broadcast_change")
    def preview_view(self, request, object_id):
        broadcast = get_object_or_404(Broadcast, pk=object_id)
        send_broadcast_task(broadcast.id, dry_run=True)
        broadcast.refresh_from_db()
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
        return redirect(self._wizard_step_url(broadcast, 3))

    @admin_action_permission("broadcast_change")
    def send_test_view(self, request, object_id):
        broadcast = get_object_or_404(Broadcast, pk=object_id)
        if not request.user.is_staff:
            messages.error(request, "Лише staff-користувачі можуть надсилати тест розсилки.")
            return redirect(self._wizard_step_url(broadcast, 3))
        self._queue_broadcast_test_message(request, broadcast)
        return redirect(self._wizard_step_url(broadcast, 3))

    @admin_action_permission("broadcast_change")
    def send_view(self, request, object_id):
        broadcast = get_object_or_404(Broadcast, pk=object_id)
        recipients_count = recipient_preview_data(broadcast)["count"]
        if broadcast.status not in {Broadcast.Status.DRAFT, Broadcast.Status.FAILED, Broadcast.Status.CANCELLED}:
            messages.warning(request, f"Розсилка вже має статус: {BROADCAST_STATUS_LABELS.get(broadcast.status, broadcast.status)}.")
            return redirect(self._wizard_step_url(broadcast, 4))
        if not broadcast.last_dry_run_preview:
            messages.error(request, "Спочатку виконайте сухий прогін.")
            return redirect(self._wizard_step_url(broadcast, 3))
        if not self._broadcast_test_ready(request, broadcast):
            messages.error(request, "Потрібна підтверджена доставка тесту адміну для поточної версії розсилки. Перевірте лог повідомлення; черга або невідомий результат не є підтвердженням.")
            return redirect(self._wizard_step_url(broadcast, 3))
        form = BroadcastSendConfirmForm(request.POST or None)
        if request.method != "POST":
            context = {
                **self.admin_site.each_context(request),
                "title": "Підтвердити запуск розсилки",
                "opts": self.model._meta,
                "form": form,
                "object": broadcast,
                "submit_label": "Запустити розсилку",
                "help_text": (
                    f"Ви збираєтеся надіслати повідомлення {recipients_count} користувачам. "
                    "Спочатку рекомендується сухий прогін і тестове надсилання."
                ),
            }
            return TemplateResponse(request, "admin/object_action_form.html", context)
        if form.is_valid() and recipients_count >= LARGE_BROADCAST_CONFIRM_THRESHOLD and form.cleaned_data["confirmation_text"].strip() != "SEND":
            messages.error(request, 'Для великої розсилки введіть "SEND".')
            return redirect("admin:broadcasts_broadcast_send", object_id=broadcast.pk)
        before = {"status": broadcast.status}
        if not self._schedule_broadcast(request, broadcast):
            return redirect(self._wizard_step_url(broadcast, 4))
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="broadcast sent",
            object_type="broadcast",
            object_id=broadcast.pk,
            before=before,
            after={"status": Broadcast.Status.SCHEDULED},
        )
        messages.success(request, "Розсилку поставлено в чергу.")
        return redirect(self._wizard_step_url(broadcast, 4))

    @admin_action_permission("broadcast_change")
    def cancel_view(self, request, object_id):
        broadcast = get_object_or_404(Broadcast, pk=object_id)
        if broadcast.status in {Broadcast.Status.COMPLETED, Broadcast.Status.SENT}:
            messages.warning(request, "Завершену розсилку не можна скасувати.")
            return redirect(self._wizard_step_url(broadcast, 4))
        if request.method != "POST":
            context = {
                **self.admin_site.each_context(request),
                "title": "Скасувати розсилку",
                "opts": self.model._meta,
                "form": BroadcastSendConfirmForm(),
                "object": broadcast,
                "submit_label": "Скасувати розсилку",
                "help_text": "Після підтвердження розсилка перейде в статус 'Скасована'.",
            }
            return TemplateResponse(request, "admin/object_action_form.html", context)
        before = {"status": broadcast.status}
        broadcast.status = Broadcast.Status.CANCELLED
        broadcast.save(update_fields=["status", "updated_at"])
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="broadcast cancelled",
            object_type="broadcast",
            object_id=broadcast.pk,
            before=before,
            after={"status": Broadcast.Status.CANCELLED},
        )
        messages.success(request, "Розсилку скасовано.")
        return redirect(self._wizard_step_url(broadcast, 4))


@admin.register(BroadcastRecipient, site=admin_site)
class BroadcastRecipientAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "broadcast_recipient"
    list_display = ("broadcast", "user", "status_label", "sent_at", "error_message")
    search_fields = ("broadcast__title", "user__username", "user__tg_user_id", "error_message")
    list_filter = ("status", "sent_at", "created_at")
    readonly_fields = ("created_at", "sent_at")

    def has_add_permission(self, request):
        return False

    @admin.display(description="Статус")
    def status_label(self, obj: BroadcastRecipient):
        return RECIPIENT_STATUS_LABELS.get(obj.status, obj.status)


@admin.register(AdminMessageLog, site=admin_site)
class AdminMessageLogAdmin(AuditedModelAdmin):
    audit_object_type = "admin_message_log"
    list_display = ("created_at", "user_display", "admin_user", "status_label", "message_preview", "sent_at")
    search_fields = ("telegram_user__username", "telegram_user__tg_user_id", "error_message")
    list_filter = ("status", "parse_mode", "created_at", "sent_at")
    readonly_fields = ("created_at", "sent_at", "response_payload", "buttons_payload")

    def has_add_permission(self, request):
        return False

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("telegram_user", "admin_user")

    @admin.display(description="Користувач", ordering="telegram_user__username")
    def user_display(self, obj: AdminMessageLog):
        return user_identity(obj.telegram_user) if obj.telegram_user else f"Chat ID {obj.target_chat_id or '—'}"

    @admin.display(description="Статус")
    def status_label(self, obj: AdminMessageLog):
        return status_badge(obj.status, MESSAGE_STATUS_LABELS.get(obj.status, obj.status))

    @admin.display(description="Прев'ю")
    def message_preview(self, obj: AdminMessageLog):
        return compact_text(obj.message_text, limit=100)
