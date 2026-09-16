from __future__ import annotations

import csv
import json
import uuid
from datetime import timedelta
from decimal import Decimal

from django import forms
from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.helpers import ActionForm
from django.db.models import OuterRef, Prefetch, Subquery
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html, format_html_join

from bot_events.models import BotEvent
from broadcasts.models import AdminMessageLog, Broadcast
from broadcasts.tasks import send_manual_message_task
from common.admin import AuditedModelAdmin
from common.admin_actions import admin_action_permission, require_admin_action, require_non_operator_target
from common.admin_site import admin_site
from common.admin_ui import boolean_badge
from common.audit import create_audit_log
from subscriptions.billing import get_latest_refundable_monobank_payment, refund_latest_monobank_payment, retry_monobank_charge
from subscriptions.monobank import MonobankAPIError
from subscriptions.models import BillingProfile, Payment, Plan, Subscription
from subscriptions.services import apply_subscription_change, resolve_subscription_plan_ref
from users.models import AdminNote, PushTopic, Tag, TelegramUser, UserAdminState, UserTag
from users.services import (
    REPLAY_RESET_CONFIRMATION,
    RESET_MODES,
    HardDeleteUserError,
    ResetOnboardingError,
    assign_tag_to_users,
    get_or_create_user_tag,
    hard_delete_user,
    is_allowlisted_test_user,
    mark_user_as_test_user,
    remove_tag_from_users,
    reset_user_onboarding,
    unmark_user_as_test_user,
)


TelegramUser._meta.verbose_name = "Користувач"
TelegramUser._meta.verbose_name_plural = "Користувачі"
PushTopic._meta.verbose_name = "Тема повідомлень"
PushTopic._meta.verbose_name_plural = "Теми повідомлень"
Tag._meta.verbose_name = "Тег"
Tag._meta.verbose_name_plural = "Теги"
USER_STATUS_LABELS = {
    UserAdminState.Status.ACTIVE: "Активний",
    UserAdminState.Status.BANNED: "Заблокований",
    UserAdminState.Status.INACTIVE: "Неактивний",
}
SUBSCRIPTION_STATUS_LABELS = {
    UserAdminState.SubscriptionStatus.NONE: "Немає",
    UserAdminState.SubscriptionStatus.TRIAL: "Тріал",
    UserAdminState.SubscriptionStatus.PAID: "Платна",
    UserAdminState.SubscriptionStatus.EXPIRED: "Завершена",
    UserAdminState.SubscriptionStatus.CANCELLED: "Скасована",
}
SUBSCRIPTION_ACTION_LABELS = {
    "trial": "Видати тріал",
    "manual": "Надати / продовжити платний доступ",
    "extend": "Продовжити підписку",
    "expire": "Завершити підписку",
    "cancel": "Скасувати підписку",
}
SUBSCRIPTION_RECORD_STATUS_LABELS = {
    Subscription.Status.TRIAL: "Тріал",
    Subscription.Status.ACTIVE: "Активна",
    Subscription.Status.PAID: "Платна",
    Subscription.Status.EXPIRED: "Завершена",
    Subscription.Status.CANCELLED: "Скасована",
    Subscription.Status.MANUAL: "Ручна",
    Subscription.Status.LIFETIME: "Довічна",
}
ACCESS_SCOPE_LABELS = {
    UserAdminState.AccessScope.PERSONAL_FULL: "Personal full",
    UserAdminState.AccessScope.FAMILY_FULL: "Family full",
    UserAdminState.AccessScope.DEBT_ONLY: "Debt only",
    UserAdminState.AccessScope.PAYWALL: "Paywall",
}
ADMIN_MESSAGE_STATUS_LABELS = {
    AdminMessageLog.Status.QUEUED: "У черзі",
    AdminMessageLog.Status.SENT: "Надіслано",
    AdminMessageLog.Status.FAILED: "Помилка",
    AdminMessageLog.Status.BLOCKED: "Заблокував бота",
    AdminMessageLog.Status.SKIPPED: "Пропущено",
}
BOT_EVENT_LABELS = {
    "start": "Команда /start",
    "new_user_registered": "Новий користувач",
    "onboarding_started": "Початок онбордингу",
    "onboarding_step_completed": "Крок онбордингу завершено",
    "onboarding_completed": "Онбординг завершено",
    "onboarding_failed": "Помилка онбордингу",
    "onboarding_reset_by_admin": "Онбординг скинув адмін",
    "text_received": "Отримано текст",
    "voice_received": "Отримано голос",
    "button_clicked": "Натиснуто кнопку",
    "transaction_created": "Створено транзакцію",
    "transaction_failed": "Помилка транзакції",
    "category_created": "Створено категорію",
    "category_updated": "Оновлено категорію",
    "account_created": "Створено рахунок",
    "subscription_started": "Підписка активована",
    "subscription_expired": "Підписка завершилась",
    "payment_success": "Успішна оплата",
    "payment_failed": "Помилка оплати",
    "parse_error": "Помилка парсингу",
    "bot_error": "Помилка бота",
    "manual_message_sent": "Ручне повідомлення",
    "broadcast_completed": "Розсилку завершено",
    "broadcast_failed": "Помилка розсилки",
    "healthcheck_run": "Перевірка системи",
}
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


class ManualMessageForm(forms.Form):
    message_text = forms.CharField(widget=forms.Textarea(attrs={"rows": 8}), max_length=4000, label="Текст повідомлення")
    parse_mode = forms.ChoiceField(choices=Broadcast.ParseMode.choices, initial=Broadcast.ParseMode.NONE, label="Формат")
    send_test_to_admin_first = forms.BooleanField(required=False, initial=False, label="Спочатку надіслати тест адміну")


class SubscriptionActionForm(forms.Form):
    plan_ref = forms.ModelChoiceField(queryset=Plan.objects.none(), required=False, label="Тариф")
    days = forms.IntegerField(required=False, min_value=1, initial=30, label="Кількість днів")
    amount = forms.DecimalField(required=False, min_value=0, decimal_places=2, max_digits=12, label="Сума")
    currency = forms.CharField(required=False, initial="USD", label="Валюта")
    comment = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Коментар")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields.pop("amount", None)
        self.fields.pop("currency", None)
        self.fields["plan_ref"].queryset = Plan.objects.filter(is_active=True, is_archived=False).order_by("display_order", "name")


class ConfirmActionForm(forms.Form):
    comment = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Коментар")


class RefundLatestPaymentForm(forms.Form):
    confirm = forms.BooleanField(
        required=True,
        label="Підтверджую повне повернення вказаного Monobank списання.",
    )
    reason = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Коментар для аудиту")


class ForceChargeNowForm(forms.Form):
    intent_key = forms.CharField(required=True, widget=forms.HiddenInput)
    confirm = forms.BooleanField(
        required=True,
        label="Підтверджую, що треба негайно запустити повне списання із збереженої картки.",
    )
    reason = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Коментар для аудиту")


class AdminNoteForm(forms.Form):
    note_text = forms.CharField(widget=forms.Textarea(attrs={"rows": 6}), max_length=4000, label="Нотатка адміна")


class TelegramUserActionForm(ActionForm):
    tag = forms.ModelChoiceField(queryset=Tag.objects.none(), required=False, label="\u0422\u0435\u0433")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["tag"].queryset = Tag.objects.order_by("name")


class TestUserForm(forms.Form):
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 4}), label="Нотатка про тестового користувача")


class ResetOnboardingForm(forms.Form):
    mode = forms.ChoiceField(choices=RESET_MODES, initial="safe", label="Режим скидання")
    reason = forms.CharField(widget=forms.Textarea(attrs={"rows": 4}), label="Причина")
    clear_fsm_state = forms.BooleanField(required=False, initial=True, label="Очистити FSM / тимчасовий стан")
    send_telegram_notice = forms.BooleanField(required=False, initial=True, label="Надіслати підказку користувачу в Telegram")
    send_admin_notification = forms.BooleanField(required=False, initial=True, label="Надіслати службовий пуш адміну")
    double_confirmed = forms.BooleanField(required=False, initial=False, label="Підтверджую небезпечну дію")
    confirmation_text = forms.CharField(required=False, label='Підтвердження для повного скидання: "RESET USER"')


class EraseUserDataForm(forms.Form):
    reason = forms.CharField(widget=forms.Textarea(attrs={"rows": 4}), label="Причина")
    clear_fsm_state = forms.BooleanField(required=False, initial=True, label="Очистити FSM / тимчасовий стан")
    send_telegram_notice = forms.BooleanField(required=False, initial=True, label="Надіслати підказку користувачу в Telegram")
    send_admin_notification = forms.BooleanField(required=False, initial=True, label="Надіслати службовий пуш адміну")
    double_confirmed = forms.BooleanField(required=False, initial=False, label="Підтверджую стирання даних користувача")
    confirmation_text = forms.CharField(required=False, label=f'Підтвердження: "{REPLAY_RESET_CONFIRMATION}"')


class HardDeleteUserForm(forms.Form):
    reason = forms.CharField(widget=forms.Textarea(attrs={"rows": 4}), label="Причина")
    double_confirmed = forms.BooleanField(required=False, initial=False, label="Підтверджую безповоротне видалення")
    confirmation_text = forms.CharField(required=False, label='Підтвердження: "DELETE USER <telegram_id>"')


class UserStatusListFilter(admin.SimpleListFilter):
    title = "статус"
    parameter_name = "status"

    def lookups(self, request, model_admin):
        return (
            (UserAdminState.Status.ACTIVE, "Активний"),
            (UserAdminState.Status.BANNED, "Заблокований"),
            (UserAdminState.Status.INACTIVE, "Неактивний"),
        )

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(admin_state__status=self.value())
        return queryset


class ActiveStateListFilter(admin.SimpleListFilter):
    title = "активність"
    parameter_name = "activity"

    def lookups(self, request, model_admin):
        return (
            ("active", "Активні за 7 днів"),
            ("inactive", "Неактивні 30 днів"),
            ("blocked_bot", "Заблокували бота"),
        )

    def queryset(self, request, queryset):
        now = timezone.now()
        if self.value() == "active":
            return queryset.filter(last_seen_at__gte=now - timedelta(days=7))
        if self.value() == "inactive":
            return queryset.filter(last_seen_at__lt=now - timedelta(days=30))
        if self.value() == "blocked_bot":
            return queryset.filter(admin_state__blocked_bot=True)
        return queryset


class OnboardingListFilter(admin.SimpleListFilter):
    title = "онбординг"
    parameter_name = "onboarding"

    def lookups(self, request, model_admin):
        return (("done", "Завершено"), ("pending", "Не завершено"))

    def queryset(self, request, queryset):
        if self.value() == "done":
            return queryset.filter(onboarding_completed=True)
        if self.value() == "pending":
            return queryset.filter(onboarding_completed=False)
        return queryset


class SubscriptionStateListFilter(admin.SimpleListFilter):
    title = "підписка"
    parameter_name = "subscription_status"

    def lookups(self, request, model_admin):
        return (
            (UserAdminState.SubscriptionStatus.NONE, "Немає"),
            (UserAdminState.SubscriptionStatus.TRIAL, "Тріал"),
            (UserAdminState.SubscriptionStatus.PAID, "Платна"),
            (UserAdminState.SubscriptionStatus.EXPIRED, "Завершена"),
            (UserAdminState.SubscriptionStatus.CANCELLED, "Скасована"),
        )

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(admin_state__subscription_status=self.value())
        return queryset


class TestUserListFilter(admin.SimpleListFilter):
    title = "тестовий користувач"
    parameter_name = "is_test_user"

    def lookups(self, request, model_admin):
        return (("yes", "Так"), ("no", "Ні"))

    def queryset(self, request, queryset):
        if self.value() == "yes":
            return queryset.filter(admin_state__is_test_user=True)
        if self.value() == "no":
            return queryset.exclude(admin_state__is_test_user=True)
        return queryset


class TagListFilter(admin.SimpleListFilter):
    title = "тег"
    parameter_name = "tag"

    def lookups(self, request, model_admin):
        return [(str(tag.pk), tag.name) for tag in Tag.objects.order_by("name")]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(user_tags__tag_id=self.value()).distinct()
        return queryset




@admin.action(description="Позначити як тестового")
def mark_as_test_users(modeladmin, request, queryset):
    for user in queryset:
        mark_user_as_test_user(user=user, admin_user=request.user, request=request)
    messages.success(request, "Обраних користувачів позначено як тестових.")


@admin.action(description="Зняти позначку тестового")
def unmark_test_users(modeladmin, request, queryset):
    for user in queryset:
        unmark_user_as_test_user(user=user, admin_user=request.user, request=request)
    messages.success(request, "Позначку тестового користувача знято.")


@admin.action(description="Експортувати CSV")
def export_users_csv(modeladmin, request, queryset):
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="cashflow_users.csv"'
    writer = csv.writer(response)
    writer.writerow(
        [
            "telegram_id",
            "username",
            "first_name",
            "last_name",
            "status",
            "onboarding_completed",
            "subscription_status",
            "is_test_user",
            "created_at",
            "last_seen_at",
        ]
    )
    for user in queryset.select_related("admin_state").order_by("tg_user_id"):
        state = getattr(user, "admin_state", None)
        writer.writerow(
            [
                user.tg_user_id,
                user.username or "",
                user.first_name or "",
                user.last_name or "",
                USER_STATUS_LABELS.get(state.status if state else UserAdminState.Status.ACTIVE, "Активний"),
                user.onboarding_completed,
                SUBSCRIPTION_STATUS_LABELS.get(
                    state.subscription_status if state else UserAdminState.SubscriptionStatus.NONE,
                    "Немає",
                ),
                bool(state and state.is_test_user),
                user.created_at.isoformat() if user.created_at else "",
                user.last_seen_at.isoformat() if user.last_seen_at else "",
            ]
        )
    return response


@admin.register(Tag, site=admin_site)
class TagAdmin(AuditedModelAdmin):
    audit_object_type = "tag"
    list_display = ("name", "slug", "color_display", "created_at")
    search_fields = ("name", "slug", "description")

    @admin.display(description="Колір", ordering="color")
    def color_display(self, obj: Tag):
        return format_html(
            '<span style="display:inline-flex;align-items:center;gap:7px"><i style="width:12px;height:12px;border-radius:50%;background:{}"></i>{}</span>',
            obj.color or "#64748b",
            obj.color or "—",
        )


@admin.register(PushTopic, site=admin_site)
class PushTopicAdmin(AuditedModelAdmin):
    audit_object_type = "push_topic"
    list_display = ("name", "slug", "system_display", "active_display", "created_at")
    search_fields = ("name", "slug", "description")
    list_filter = ("is_system", "is_active")
    readonly_fields = ("created_at",)

    @admin.display(description="Системна", ordering="is_system")
    def system_display(self, obj: PushTopic):
        return boolean_badge(obj.is_system)

    @admin.display(description="Стан", ordering="is_active")
    def active_display(self, obj: PushTopic):
        return boolean_badge(obj.is_active, true_label="Активна", false_label="Неактивна")


@admin.register(TelegramUser, site=admin_site)
class TelegramUserAdmin(AuditedModelAdmin):
    audit_object_type = "telegram_user"
    change_form_template = "admin/user_crm_detail.html"
    change_list_template = "admin/users_changelist.html"
    list_display = (
        "action_checkbox",
        "username_display",
        "full_name_display",
        "tags_display",
        "user_status",
        "subscription_status",
        "last_seen_short",
        "quick_actions",
    )
    search_fields = ("tg_user_id", "username", "first_name", "last_name")
    list_filter = (
        UserStatusListFilter,
        ActiveStateListFilter,
        OnboardingListFilter,
        SubscriptionStateListFilter,
        TagListFilter,
    )
    list_display_links = ("username_display", "full_name_display")
    list_per_page = 25
    actions = None
    readonly_fields = (
        "tg_user_id",
        "username_display",
        "full_name_display",
        "first_name",
        "last_name",
        "lang",
        "base_currency",
        "created_at",
        "last_seen_at",
        "user_status",
        "access_scope_display",
        "onboarding_status_display",
        "current_onboarding_step_display",
        "subscription_status",
        "subscription_end_at",
        "is_test_user_display",
        "timezone_display",
        "last_action_at_display",
        "current_subscription_preview",
        "recent_actions_preview",
        "admin_notes_preview",
        "admin_actions_panel",
    )
    fieldsets = (
        (
            "Профіль",
            {
                "fields": (
                    "tg_user_id",
                    "username_display",
                    "full_name_display",
                    "first_name",
                    "last_name",
                    "lang",
                    "base_currency",
                    "timezone_display",
                    "created_at",
                    "last_seen_at",
                    "is_test_user_display",
                )
            },
        ),
        (
            "Статус і підписка",
            {
                "fields": (
                    "user_status",
                    "access_scope_display",
                    "subscription_status",
                    "subscription_end_at",
                    "current_subscription_preview",
                    "admin_actions_panel",
                )
            },
        ),
        (
            "Онбординг",
            {
                "fields": (
                    "onboarding_status_display",
                    "current_onboarding_step_display",
                )
            },
        ),
        (
            "Активність",
            {
                "fields": (
                    "last_action_at_display",
                    "recent_actions_preview",
                )
            },
        ),
        ("Нотатки адміна", {"fields": ("admin_notes_preview",)}),
    )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    @admin_action_permission("user_view")
    def change_view(self, request, object_id, form_url="", extra_context=None):
        extra_context = extra_context or {}
        user = self.get_queryset(request).filter(pk=object_id).first()
        if user is not None:
            extra_context["crm"] = self._build_crm_context(user)
        return super().change_view(request, object_id, form_url, extra_context=extra_context)

    def get_queryset(self, request):
        latest_subscription_qs = Subscription.objects.filter(user_id=OuterRef("tg_user_id")).order_by("-created_at")
        qs = super().get_queryset(request).select_related("admin_state").prefetch_related(
            Prefetch(
                "user_tags",
                queryset=UserTag.objects.select_related("tag").order_by("tag__name"),
            )
        )
        return qs.annotate(annotated_next_billing_at=Subquery(latest_subscription_qs.values("expires_at")[:1]))

    def changelist_view(self, request, extra_context=None):
        if request.method == "POST" and request.POST.get("bulk_tag_submit") == "1":
            self._handle_bulk_tag_post(request)
            return redirect(request.get_full_path())

        extra_context = extra_context or {}
        extra_context["bulk_tags"] = Tag.objects.order_by("name")
        extra_context["bulk_tags_url"] = reverse("admin:users_tag_changelist")
        return super().changelist_view(request, extra_context=extra_context)

    def _handle_bulk_tag_post(self, request):
        operation = str(request.POST.get("bulk_tag_operation") or "").strip()
        actions = {
            "create_only": ("tag_create",), "create_assign": ("tag_create", "tag_assign"),
            "assign": ("tag_assign",), "remove": ("tag_remove",),
        }.get(operation, ("unknown",))
        for action in actions:
            require_admin_action(getattr(request, "user", None), action)
        selected_ids = [str(value).strip() for value in request.POST.getlist("_selected_action") if str(value).strip()]
        operation = str(request.POST.get("bulk_tag_operation") or "").strip()
        tag_id = str(request.POST.get("bulk_tag") or "").strip()
        new_tag_name = str(request.POST.get("bulk_tag_new_name") or "").strip()

        if operation == "create_only":
            if not new_tag_name:
                messages.error(request, "\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043d\u0430\u0437\u0432\u0443 \u043d\u043e\u0432\u043e\u0433\u043e \u0442\u0435\u0433\u0430.")
                return
            try:
                tag, created = get_or_create_user_tag(name=new_tag_name, admin_user=request.user, request=request)
            except ValueError:
                messages.error(request, "\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043d\u0430\u0437\u0432\u0443 \u043d\u043e\u0432\u043e\u0433\u043e \u0442\u0435\u0433\u0430.")
                return
            if created:
                messages.success(request, f'\u0422\u0435\u0433 \u00ab{tag.name}\u00bb \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043e.')
            else:
                messages.info(request, f'\u0422\u0435\u0433 \u00ab{tag.name}\u00bb \u0432\u0436\u0435 \u0456\u0441\u043d\u0443\u0454.')
            return

        if operation == "create_assign":
            if not selected_ids:
                messages.error(request, "\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0445\u043e\u0447\u0430 \u0431 \u043e\u0434\u043d\u043e\u0433\u043e \u043a\u043e\u0440\u0438\u0441\u0442\u0443\u0432\u0430\u0447\u0430.")
                return
            if not new_tag_name:
                messages.error(request, "\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043d\u0430\u0437\u0432\u0443 \u043d\u043e\u0432\u043e\u0433\u043e \u0442\u0435\u0433\u0430.")
                return
            try:
                tag, created = get_or_create_user_tag(name=new_tag_name, admin_user=request.user, request=request)
            except ValueError:
                messages.error(request, "\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043d\u0430\u0437\u0432\u0443 \u043d\u043e\u0432\u043e\u0433\u043e \u0442\u0435\u0433\u0430.")
                return
            users = list(self.get_queryset(request).filter(pk__in=selected_ids))
            if not users:
                messages.error(request, "\u041d\u0435 \u0432\u0434\u0430\u043b\u043e\u0441\u044f \u0437\u043d\u0430\u0439\u0442\u0438 \u043e\u0431\u0440\u0430\u043d\u0438\u0445 \u043a\u043e\u0440\u0438\u0441\u0442\u0443\u0432\u0430\u0447\u0456\u0432.")
                return
            created_count = assign_tag_to_users(
                tag=tag,
                users=users,
                admin_user=request.user,
                request=request,
            )
            if created:
                messages.success(request, f'\u0422\u0435\u0433 \u00ab{tag.name}\u00bb \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043e \u0442\u0430 \u043f\u0440\u0438\u0441\u0432\u043e\u0454\u043d\u043e {created_count} \u043a\u043e\u0440\u0438\u0441\u0442\u0443\u0432\u0430\u0447\u0430\u043c.')
            else:
                messages.success(request, f'\u041d\u0430\u044f\u0432\u043d\u0438\u0439 \u0442\u0435\u0433 \u00ab{tag.name}\u00bb \u043f\u0440\u0438\u0441\u0432\u043e\u0454\u043d\u043e {created_count} \u043a\u043e\u0440\u0438\u0441\u0442\u0443\u0432\u0430\u0447\u0430\u043c.')
            return

        if not selected_ids:
            messages.error(request, "Оберіть хоча б одного користувача.")
            return

        tag = Tag.objects.filter(pk=tag_id).first()
        if tag is None:
            messages.error(request, "Оберіть тег.")
            return

        users = list(self.get_queryset(request).filter(pk__in=selected_ids))
        if not users:
            messages.error(request, "Не вдалося знайти обраних користувачів.")
            return

        if operation == "assign":
            created_count = assign_tag_to_users(
                tag=tag,
                users=users,
                admin_user=request.user,
                request=request,
            )
            messages.success(request, f"Тег «{tag.name}» присвоєно {created_count} користувачам.")
            return

        if operation == "remove":
            removed_count = remove_tag_from_users(
                tag=tag,
                users=users,
                admin_user=request.user,
                request=request,
            )
            messages.success(request, f"Тег «{tag.name}» знято з {removed_count} користувачів.")
            return

        messages.error(request, "Невідома bulk-операція.")

    def _display_value(self, value, empty="-"):
        if value is None:
            return empty
        if isinstance(value, str) and not value.strip():
            return empty
        return value

    def _display_datetime(self, value, empty="-"):
        if not value:
            return empty
        return value.strftime("%d.%m.%Y %H:%M")

    def _crm_action_groups(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        status = state.status if state else UserAdminState.Status.ACTIVE
        subscription_status = state.subscription_status if state else UserAdminState.SubscriptionStatus.NONE
        is_test_user = bool(state and state.is_test_user)
        is_hard_delete_candidate = is_test_user or is_allowlisted_test_user(obj)
        latest_refundable_payment = get_latest_refundable_monobank_payment(user_id=obj.tg_user_id)
        billing_profile = BillingProfile.objects.filter(user_id=obj.tg_user_id).first()

        main_actions = [
            {
                "label": "Написати користувачу",
                "url": f"{reverse('admin:manual_message')}?user_lookup={obj.tg_user_id}",
                "variant": "primary",
            },
            {
                "label": "Скинути онбординг",
                "url": reverse("admin:users_telegramuser_reset_onboarding", args=[obj.pk]),
                "variant": "secondary",
            },
            {
                "label": "Додати нотатку",
                "url": reverse("admin:users_telegramuser_add_note", args=[obj.pk]),
                "variant": "secondary",
            },
            {
                "label": "Debug онбордингу",
                "url": f"{reverse('admin:onboarding_debug')}?user_id={obj.tg_user_id}",
                "variant": "ghost",
            },
        ]

        subscription_actions = []
        if subscription_status != UserAdminState.SubscriptionStatus.TRIAL:
            subscription_actions.append(
                {
                    "label": "Видати trial",
                    "url": reverse("admin:users_telegramuser_subscription_action", args=[obj.pk, "trial"]),
                    "variant": "secondary",
                }
            )
        subscription_actions.append(
            {
                "label": "Надати / продовжити платний доступ",
                "url": reverse("admin:users_telegramuser_subscription_action", args=[obj.pk, "manual"]),
                "variant": "secondary",
            }
        )
        if subscription_status in {
            UserAdminState.SubscriptionStatus.TRIAL,
            UserAdminState.SubscriptionStatus.PAID,
        }:
            subscription_actions.append(
                {
                    "label": "Завершити підписку",
                    "url": reverse("admin:users_telegramuser_subscription_action", args=[obj.pk, "expire"]),
                    "variant": "danger",
                }
            )
        if subscription_status not in {
            UserAdminState.SubscriptionStatus.NONE,
            UserAdminState.SubscriptionStatus.EXPIRED,
            UserAdminState.SubscriptionStatus.CANCELLED,
        }:
            subscription_actions.append(
                {
                    "label": "Скасувати підписку",
                    "url": reverse("admin:users_telegramuser_subscription_action", args=[obj.pk, "cancel"]),
                    "variant": "ghost",
                }
            )
        if latest_refundable_payment is not None:
            subscription_actions.append(
                {
                    "label": (
                        "Повернути зайву 1 грн"
                        if latest_refundable_payment.kind == Payment.Kind.BIND
                        else "Повернути останнє списання"
                    ),
                    "url": reverse("admin:users_telegramuser_refund_latest_payment", args=[obj.pk]),
                    "variant": "danger",
                }
            )

        if billing_profile is not None and billing_profile.card_token and billing_profile.auto_renew_enabled:
            subscription_actions.append(
                {
                    "label": "Форснути списання зараз",
                    "url": reverse("admin:users_telegramuser_force_charge", args=[obj.pk]),
                    "variant": "danger",
                }
            )

        status_actions = [
            {
                "label": "Розбанити" if status == UserAdminState.Status.BANNED else "Забанити",
                "url": reverse(
                    "admin:users_telegramuser_set_status",
                    args=[
                        obj.pk,
                        UserAdminState.Status.ACTIVE if status == UserAdminState.Status.BANNED else UserAdminState.Status.BANNED,
                    ],
                ),
                "variant": "danger" if status != UserAdminState.Status.BANNED else "secondary",
            },
            {
                "label": "Зняти тестового" if is_test_user else "Позначити як тестового",
                "url": reverse(
                    "admin:users_telegramuser_unmark_test_user" if is_test_user else "admin:users_telegramuser_mark_test_user",
                    args=[obj.pk],
                ),
                "variant": "ghost",
            },
        ]

        if is_hard_delete_candidate:
            main_actions.append(
                {
                    "label": "Стерти дані користувача",
                    "url": reverse("admin:users_telegramuser_erase_user_data", args=[obj.pk]),
                    "variant": "danger",
                }
            )

        groups = [
            {"title": "Швидкі дії", "items": main_actions},
            {"title": "Підписка", "items": subscription_actions},
            {"title": "Статус", "items": status_actions},
        ]

        if is_hard_delete_candidate:
            groups.append(
                {
                    "title": "Danger zone",
                    "items": [
                        {
                            "label": "Хардово видалити",
                            "url": reverse("admin:users_telegramuser_hard_delete", args=[obj.pk]),
                            "variant": "danger",
                        }
                    ],
                }
            )
        return groups

    def _build_crm_context(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        status = state.status if state else UserAdminState.Status.ACTIVE
        subscription_status = state.subscription_status if state else UserAdminState.SubscriptionStatus.NONE
        latest_logs = list(
            AdminMessageLog.objects.filter(telegram_user_id=obj.tg_user_id).select_related("admin_user").order_by("-created_at")[:10]
        )
        subscriptions = list(
            Subscription.objects.filter(user_id=obj.tg_user_id).select_related("plan_ref", "created_by").order_by("-created_at")[:10]
        )
        onboarding_events = list(
            BotEvent.objects.filter(user_id=obj.tg_user_id, event_type__in=ONBOARDING_EVENT_TYPES)
            .defer("raw_input", "parsed_result")
            .order_by("-created_at")[:20]
        )
        recent_events = list(
            BotEvent.objects.filter(user_id=obj.tg_user_id)
            .defer("raw_input", "parsed_result")
            .order_by("-created_at")[:10]
        )
        notes = list(obj.admin_notes.select_related("admin_user").order_by("-created_at")[:10])

        for log in latest_logs:
            log.crm_status_label = ADMIN_MESSAGE_STATUS_LABELS.get(log.status, log.status)
        for subscription in subscriptions:
            subscription.crm_status_label = SUBSCRIPTION_RECORD_STATUS_LABELS.get(subscription.status, subscription.status)
            subscription.crm_plan_label = subscription.plan_ref.name if subscription.plan_ref_id else subscription.plan
        for event in onboarding_events + recent_events:
            event.crm_event_label = BOT_EVENT_LABELS.get(event.event_type, event.event_type)
            event.crm_source_label = event.source or "система"

        last_onboarding_event = onboarding_events[0] if onboarding_events else None
        current_step = (state.current_fsm_state if state else "") or "Невідомо"

        return {
            "user": obj,
            "summary_cards": [
                {"label": "Статус", "value": USER_STATUS_LABELS.get(status, status)},
                {"label": "Підписка", "value": SUBSCRIPTION_STATUS_LABELS.get(subscription_status, subscription_status)},
                {"label": "Онбординг", "value": "Завершено" if obj.onboarding_completed else "Не завершено"},
                {"label": "Остання активність", "value": self._display_datetime(obj.last_seen_at)},
            ],
            "profile_rows": [
                ("Telegram ID", obj.tg_user_id),
                ("Нік у Telegram", f"@{obj.username}" if obj.username else "-"),
                ("Ім'я", obj.full_name),
                ("Мова", self._display_value(obj.lang)),
                ("Валюта", self._display_value(obj.base_currency)),
                ("Статус", USER_STATUS_LABELS.get(status, status)),
                ("Підписка", SUBSCRIPTION_STATUS_LABELS.get(subscription_status, subscription_status)),
                ("Підписка до", self._display_datetime(self.subscription_end_at(obj))),
                ("Бот запущено", "Так" if obj.start_date else "Ні"),
                ("Тестовий користувач", "Так" if state and state.is_test_user else "Ні"),
                ("Створено", self._display_datetime(obj.created_at)),
                ("Остання активність", self._display_datetime(obj.last_seen_at)),
            ],
            "communication_rows": [
                ("Можна писати", "Так" if not (state and state.blocked_bot) else "Ні"),
                ("Бота заблоковано", "Так" if state and state.blocked_bot else "Ні"),
                ("Остання ручна відправка", self._display_datetime(latest_logs[0].created_at) if latest_logs else "-"),
            ],
            "onboarding_rows": [
                ("Статус", "Завершено" if obj.onboarding_completed else "Не завершено"),
                ("Поточний крок", current_step),
                ("Остання onboarding-подія", last_onboarding_event.crm_event_label if last_onboarding_event else "-"),
                ("Остання помилка", self._display_value(state.last_parse_error if state else "")),
            ],
            "action_groups": self._crm_action_groups(obj),
            "subscriptions": subscriptions,
            "message_logs": latest_logs,
            "recent_events": recent_events,
            "onboarding_events": onboarding_events,
            "notes": notes,
            "links": {
                "subscriptions": f"{reverse('admin:subscriptions_subscription_changelist')}?q={obj.tg_user_id}",
                "send_message": f"{reverse('admin:manual_message')}?user_lookup={obj.tg_user_id}",
            },
        }

    def _object_action_context(self, request, *, user: TelegramUser, title: str, form, submit_label: str, help_text: str = ""):
        return {
            **self.admin_site.each_context(request),
            "title": title,
            "opts": self.model._meta,
            "form": form,
            "object": user,
            "submit_label": submit_label,
            "help_text": help_text,
            "back_url": reverse("admin:users_telegramuser_change", args=[user.pk]),
        }

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("<path:object_id>/send-message/", self.admin_site.admin_view(self.send_message_view), name="users_telegramuser_send_message"),
            path("<path:object_id>/status/<str:status>/", self.admin_site.admin_view(self.set_status_view), name="users_telegramuser_set_status"),
            path("<path:object_id>/subscription/<str:action>/", self.admin_site.admin_view(self.subscription_action_view), name="users_telegramuser_subscription_action"),
            path("<path:object_id>/force-charge/", self.admin_site.admin_view(self.force_charge_now_view), name="users_telegramuser_force_charge"),
            path("<path:object_id>/refund-latest-payment/", self.admin_site.admin_view(self.refund_latest_payment_view), name="users_telegramuser_refund_latest_payment"),
            path("<path:object_id>/reset-onboarding/", self.admin_site.admin_view(self.reset_onboarding_view), name="users_telegramuser_reset_onboarding"),
            path("<path:object_id>/erase-user-data/", self.admin_site.admin_view(self.erase_user_data_view), name="users_telegramuser_erase_user_data"),
            path("<path:object_id>/hard-delete/", self.admin_site.admin_view(self.hard_delete_user_view), name="users_telegramuser_hard_delete"),
            path("<path:object_id>/add-note/", self.admin_site.admin_view(self.add_note_view), name="users_telegramuser_add_note"),
            path("<path:object_id>/mark-test-user/", self.admin_site.admin_view(self.mark_test_user_view), name="users_telegramuser_mark_test_user"),
            path("<path:object_id>/unmark-test-user/", self.admin_site.admin_view(self.unmark_test_user_view), name="users_telegramuser_unmark_test_user"),
        ]
        return custom_urls + urls

    @admin.display(description="Внутрішній ID")
    def internal_id_value(self, obj: TelegramUser):
        return obj.internal_id

    @admin.display(description="Нік у Telegram")
    def username_display(self, obj: TelegramUser):
        return f"@{obj.username}" if obj.username else "-"

    @admin.display(description="Ім'я")
    def full_name_display(self, obj: TelegramUser):
        return obj.full_name

    @admin.display(description="Теги")
    def tags_display(self, obj: TelegramUser):
        prefetched_tags = getattr(obj, "_prefetched_objects_cache", {}).get("user_tags")
        links = prefetched_tags if prefetched_tags is not None else obj.user_tags.select_related("tag").order_by("tag__name")
        names = [link.tag.name for link in links if getattr(link, "tag", None)]
        return ", ".join(names) if names else "-"

    @admin.display(description="РЎС‚Р°С‚СѓСЃ")
    def user_status(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        status = state.status if state else UserAdminState.Status.ACTIVE
        return USER_STATUS_LABELS.get(status, status)

    @admin.display(description="Онбординг")
    def onboarding_status_display(self, obj: TelegramUser):
        return "Завершено" if obj.onboarding_completed else "Не завершено"

    @admin.display(description="Поточний крок")
    def current_onboarding_step_display(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        return (state.current_fsm_state if state else "") or "Невідомо"

    @admin.display(description="Підписка")
    def subscription_status(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        status = state.subscription_status if state else UserAdminState.SubscriptionStatus.NONE
        return SUBSCRIPTION_STATUS_LABELS.get(status, status)

    @admin.display(description="Access")
    def access_scope_display(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        scope = state.access_scope if state else UserAdminState.AccessScope.PAYWALL
        return ACCESS_SCOPE_LABELS.get(scope, scope)

    @admin.display(description="Наступне списання", ordering="annotated_next_billing_at")
    def next_billing_at(self, obj: TelegramUser):
        return getattr(obj, "annotated_next_billing_at", None)

    @admin.display(description="Підписка до", ordering="annotated_next_billing_at")
    def subscription_end_at(self, obj: TelegramUser):
        annotated_value = getattr(obj, "annotated_next_billing_at", None)
        if annotated_value is not None:
            return annotated_value
        latest = Subscription.objects.filter(user_id=obj.tg_user_id).order_by("-created_at").first()
        return latest.expires_at if latest else None

    @admin.display(description="\u0422\u0435\u0433\u0438")
    def tags_display(self, obj: TelegramUser):
        prefetched_tags = getattr(obj, "_prefetched_objects_cache", {}).get("user_tags")
        links = prefetched_tags if prefetched_tags is not None else obj.user_tags.select_related("tag").order_by("tag__name")
        items = []
        palette = (
            ("#eef6ff", "#bfdbfe", "#1d4ed8"),
            ("#f0fdf4", "#bbf7d0", "#166534"),
            ("#fff7ed", "#fed7aa", "#c2410c"),
            ("#fefce8", "#fde68a", "#854d0e"),
            ("#fdf2f8", "#fbcfe8", "#be185d"),
            ("#eef2ff", "#c7d2fe", "#4338ca"),
        )
        for link in links:
            tag = getattr(link, "tag", None)
            if tag is None:
                continue
            idx = sum(ord(char) for char in tag.name) % len(palette)
            background, border, text = palette[idx]
            items.append((background, border, text, tag.name))
        if not items:
            return "-"
        return format_html_join(
            "",
            "<span class='mcf-tag-badge' style='--tag-bg:{}; --tag-border:{}; --tag-text:{};'>{}</span>",
            items,
        )

    @admin.display(description="\u0421\u0442\u0430\u0442\u0443\u0441")
    def user_status(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        status = state.status if state else UserAdminState.Status.ACTIVE
        return USER_STATUS_LABELS.get(status, status)

    @admin.display(description="\u041d\u0430\u0441\u0442\u0443\u043f\u043d\u0435 \u0441\u043f\u0438\u0441\u0430\u043d\u043d\u044f", ordering="annotated_next_billing_at")
    def next_billing_at(self, obj: TelegramUser):
        return getattr(obj, "annotated_next_billing_at", None)

    @admin.display(description="\u041f\u0456\u0434\u043f\u0438\u0441\u043a\u0430 \u0434\u043e", ordering="annotated_next_billing_at")
    def subscription_end_at(self, obj: TelegramUser):
        annotated_value = getattr(obj, "annotated_next_billing_at", None)
        if annotated_value is not None:
            return annotated_value
        latest = Subscription.objects.filter(user_id=obj.tg_user_id).order_by("-created_at").first()
        return latest.expires_at if latest else None

    @admin.display(description="Часовий пояс")
    def timezone_display(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        return state.timezone if state and state.timezone else "-"

    @admin.display(description="Остання дія")
    def last_action_at_display(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        return state.last_action_at if state else None

    @admin.display(description="Створено")
    def created_at_short(self, obj: TelegramUser):
        return obj.created_at.strftime("%Y-%m-%d %H:%M") if obj.created_at else "-"

    @admin.display(description="Остання активність")
    def last_seen_short(self, obj: TelegramUser):
        return obj.last_seen_at.strftime("%Y-%m-%d %H:%M") if obj.last_seen_at else "-"

    @admin.display(boolean=True, description="Тестовий")
    def is_test_user_display(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        return bool(state and state.is_test_user)

    @admin.display(description="Підписка")
    def current_subscription_preview(self, obj: TelegramUser):
        subscriptions = Subscription.objects.filter(user_id=obj.tg_user_id).select_related("plan_ref").order_by("-created_at")[:10]
        if not subscriptions:
            return "Підписок поки немає."
        return format_html_join(
            "\n",
            "<div><strong>{}</strong> / {} / {} / до {}</div>",
            (
                (
                    item.plan_ref.name if item.plan_ref_id else item.plan,
                    SUBSCRIPTION_RECORD_STATUS_LABELS.get(item.status, item.status),
                    item.provider or "-",
                    item.expires_at or "-",
                )
                for item in subscriptions
            ),
        )

    @admin.display(description="Останні 20 подій")
    def recent_actions_preview(self, obj: TelegramUser):
        rows = BotEvent.objects.filter(user_id=obj.tg_user_id).order_by("-created_at")[:20]
        if not rows:
            return "Подій поки немає."
        return format_html_join(
            "\n",
            "<div><strong>{}</strong> [{}] {} {}</div>",
            (
                (
                    row.event_type,
                    row.created_at.strftime("%Y-%m-%d %H:%M"),
                    row.source or "система",
                    row.error_message or "",
                )
                for row in rows
            ),
        )

    @admin.display(description="Нотатки адміна")
    def admin_notes_preview(self, obj: TelegramUser):
        notes = obj.admin_notes.select_related("admin_user").order_by("-created_at")[:10]
        add_url = reverse("admin:users_telegramuser_add_note", args=[obj.pk])
        if not notes:
            return format_html("<div>Нотаток поки немає.</div><div style='margin-top:8px;'><a class='button' href='{}'>Додати нотатку</a></div>", add_url)
        rows = [
            format_html("<div style='margin-bottom:8px;'><strong>{}</strong> [{}]<br>{}</div>", note.admin_user, note.created_at.strftime("%Y-%m-%d %H:%M"), note.note_text)
            for note in notes
        ]
        rows.append(format_html("<a class='button' href='{}'>Додати нотатку</a>", add_url))
        return format_html("".join(str(row) for row in rows))

    @admin.display(description="Debug онбордингу")
    def onboarding_debug_preview(self, obj: TelegramUser):
        state = getattr(obj, "admin_state", None)
        onboarding_events = BotEvent.objects.filter(user_id=obj.tg_user_id, event_type__in=ONBOARDING_EVENT_TYPES).order_by("-created_at")[:20]
        last_event = onboarding_events[0] if onboarding_events else None
        summary = format_html_join(
            "",
            "<div><strong>{}:</strong> {}</div>",
            (
                ("завершений", obj.onboarding_completed),
                ("версія", obj.onboarding_version),
                ("поточний FSM стан", (state.current_fsm_state if state else "") or "-"),
                ("остання onboarding-подія", last_event.event_type if last_event else "-"),
                ("останній input користувача", (state.last_user_input if state else "") or "-"),
                ("остання відповідь бота", (state.last_bot_response if state else "") or "-"),
                ("остання помилка парсингу", (state.last_parse_error if state else "") or "-"),
                ("час останньої onboarding-події", (state.last_onboarding_event_at if state else None) or "-"),
            ),
        )
        payload_text = json.dumps(state.onboarding_payload if state else {}, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        if onboarding_events:
            events_html = format_html_join(
                "",
                "<div>[{}] <strong>{}</strong> / {} / {}</div>",
                (
                    (
                        row.created_at.strftime("%Y-%m-%d %H:%M"),
                        row.event_type,
                        row.source or "-",
                        row.error_message or "-",
                    )
                    for row in onboarding_events
                ),
            )
        else:
            events_html = format_html("<div>Onboarding-подій поки немає.</div>")
        return format_html(
            "<div>{}</div><div style='margin-top:10px;'><strong>Тимчасовий payload онбордингу</strong></div><pre>{}</pre><div style='margin-top:10px;'><strong>Останні 20 onboarding-подій</strong></div>{}",
            summary,
            payload_text,
            events_html,
        )

    @admin.display(description="Дії")
    def quick_actions(self, obj: TelegramUser):
        open_url = reverse("admin:users_telegramuser_change", args=[obj.pk])
        send_url = f"{reverse('admin:manual_message')}?user_lookup={obj.tg_user_id}"
        subscription_url = f"{reverse('admin:subscriptions_subscription_changelist')}?q={obj.tg_user_id}"
        reset_url = reverse("admin:users_telegramuser_reset_onboarding", args=[obj.pk])
        return format_html(
            '<div style="display:flex; gap:8px; flex-wrap:wrap;">'
            '<a class="button" href="{}">Відкрити</a>'
            '<a class="button" href="{}">Написати</a>'
            '<a class="button" href="{}">Підписка</a>'
            '<a class="button" href="{}">Скинути онбординг</a>'
            "</div>",
            open_url,
            send_url,
            subscription_url,
            reset_url,
        )

    @admin.display(description="Швидкі дії")
    def admin_actions_panel(self, obj: TelegramUser):
        items = [item for group in self._crm_action_groups(obj) for item in group["items"]]
        return format_html_join(
            "",
            '<a class="button" href="{}" style="margin:0 8px 8px 0;">{}</a>',
            ((item["url"], item["label"]) for item in items),
        )

    @admin_action_permission("message_send")
    def send_message_view(self, request, object_id):
        user = get_object_or_404(TelegramUser, pk=object_id)
        return redirect(f"{reverse('admin:manual_message')}?user_lookup={user.tg_user_id}")

    @admin_action_permission("user_status")
    def set_status_view(self, request, object_id, status):
        if status == UserAdminState.Status.BANNED:
            require_non_operator_target(object_id)
        user = get_object_or_404(TelegramUser, pk=object_id)
        form = ConfirmActionForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            state, _ = UserAdminState.objects.get_or_create(telegram_user_id=user.tg_user_id)
            before = {"status": state.status}
            state.status = status
            state.is_blocked = status == UserAdminState.Status.BANNED
            state.save(update_fields=["status", "is_blocked", "updated_at"])
            create_audit_log(
                request=request,
                admin_user=request.user,
                action="user_banned" if status == UserAdminState.Status.BANNED else "user_unbanned",
                object_type="telegram_user",
                object_id=user.pk,
                target_user_id=user.tg_user_id,
                reason=form.cleaned_data.get("comment", ""),
                before=before,
                after={"status": state.status},
            )
            messages.success(request, f"Статус користувача оновлено: {USER_STATUS_LABELS.get(state.status, state.status)}.")
            return redirect("admin:users_telegramuser_change", object_id=user.pk)

        context = {
            **self.admin_site.each_context(request),
            "title": "Підтвердити зміну статусу",
            "opts": self.model._meta,
            "form": form,
            "object": user,
            "submit_label": "Підтвердити",
        }
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("subscription_change")
    def subscription_action_view(self, request, object_id, action):
        user = get_object_or_404(TelegramUser, pk=object_id)
        form = SubscriptionActionForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            latest = Subscription.objects.filter(user_id=user.tg_user_id).order_by("-created_at").first()
            plan_ref = form.cleaned_data.get("plan_ref") or resolve_subscription_plan_ref(latest)
            plan_slug = plan_ref.slug if plan_ref else (latest.plan if latest else "solo")
            subscription = apply_subscription_change(
                user=user,
                action=action,
                admin_user=request.user,
                plan_slug=plan_slug,
                plan_ref=plan_ref,
                days=form.cleaned_data.get("days"),
                comment=form.cleaned_data.get("comment") or "",
            )
            create_audit_log(
                request=request,
                admin_user=request.user,
                action="subscription_changed",
                object_type="subscription",
                object_id=subscription.pk,
                target_user_id=user.tg_user_id,
                mode=action,
                reason=form.cleaned_data.get("comment", ""),
                before={},
                after={"status": subscription.status, "expires_at": subscription.expires_at.isoformat() if subscription.expires_at else None},
            )
            messages.success(request, "Дію з підпискою виконано.")
            return redirect("admin:users_telegramuser_change", object_id=user.pk)

        context = {
            **self.admin_site.each_context(request),
            "title": SUBSCRIPTION_ACTION_LABELS.get(action, action),
            "opts": self.model._meta,
            "form": form,
            "object": user,
            "submit_label": "Підтвердити",
        }
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("payment_change")
    def force_charge_now_view(self, request, object_id):
        user = get_object_or_404(TelegramUser, pk=object_id)
        profile = BillingProfile.objects.filter(user_id=user.tg_user_id).first()
        if profile is None or not profile.card_token:
            messages.error(request, "У користувача немає збереженої картки для списання.")
            return redirect("admin:users_telegramuser_change", object_id=user.pk)
        if not profile.auto_renew_enabled:
            messages.error(request, "Автопродовження вимкнене. Спершу треба знову прив'язати картку або увімкнути auto-renew.")
            return redirect("admin:users_telegramuser_change", object_id=user.pk)

        latest_subscription = Subscription.objects.filter(user_id=user.tg_user_id).order_by("-created_at").first()
        form = ForceChargeNowForm(
            request.POST or None,
            initial={"intent_key": f"admin_force_{uuid.uuid4().hex}"},
        )
        if request.method == "POST" and form.is_valid():
            before = {
                "profile_status": profile.status,
                "auto_renew_enabled": profile.auto_renew_enabled,
                "last_charge_status": profile.last_charge_status,
                "last_failure_reason": profile.last_failure_reason,
                "subscription_status": latest_subscription.status if latest_subscription is not None else "",
                "subscription_expires_at": latest_subscription.expires_at if latest_subscription is not None else None,
                "subscription_next_charge_at": latest_subscription.next_charge_at if latest_subscription is not None else None,
                "subscription_grace_expires_at": latest_subscription.grace_expires_at if latest_subscription is not None else None,
            }
            try:
                intent_key = str(form.cleaned_data.get("intent_key") or "").strip()
                result = retry_monobank_charge(user_id=user.tg_user_id, intent_key=intent_key)
            except (ValueError, MonobankAPIError) as exc:
                if isinstance(exc, ValueError) and "pending" in str(exc).lower():
                    messages.warning(
                        request,
                        "Для цього користувача вже є незавершене списання. Дочекайтесь фінального статусу, перш ніж запускати нове.",
                    )
                else:
                    messages.error(request, f"Не вдалося запустити списання: {exc}")
            else:
                profile.refresh_from_db()
                payment = Payment.objects.filter(pk=result.get("payment_id")).first()
                subscription_after = Subscription.objects.filter(user_id=user.tg_user_id).order_by("-created_at").first()
                payment_status = payment.status if payment is not None else str(result.get("status") or "")
                action_url = payment.action_url if payment is not None else str(result.get("action_url") or "")
                failure_reason = str(profile.last_failure_reason or "").strip()
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="billing_force_charge_now",
                    object_type="payment",
                    object_id=payment.pk if payment is not None else result.get("payment_id"),
                    target_user_id=user.tg_user_id,
                    mode="force_charge_now",
                    reason=form.cleaned_data.get("reason", ""),
                    before=before,
                    after={
                        "payment_id": payment.pk if payment is not None else result.get("payment_id"),
                        "invoice_id": payment.provider_payment_id if payment is not None else result.get("invoice_id"),
                        "payment_status": payment_status,
                        "action_url": action_url,
                        "profile_status": profile.status,
                        "last_charge_status": profile.last_charge_status,
                        "last_failure_reason": failure_reason,
                        "subscription_status": subscription_after.status if subscription_after is not None else "",
                        "subscription_expires_at": subscription_after.expires_at if subscription_after is not None else None,
                        "subscription_next_charge_at": subscription_after.next_charge_at if subscription_after is not None else None,
                        "subscription_grace_expires_at": subscription_after.grace_expires_at if subscription_after is not None else None,
                    },
                )
                if action_url:
                    messages.warning(
                        request,
                        "Списання запущено, але Monobank вимагає додаткове підтвердження 3DS. Перевір платіж за посиланням.",
                    )
                elif payment_status == Payment.Status.PENDING:
                    messages.warning(
                        request,
                        "Списання запущено, але Monobank ще обробляє платіж. Не запускайте повторно, доки не буде фінального статусу.",
                    )
                elif payment_status in {Payment.Status.FAILED, Payment.Status.REJECTED}:
                    suffix = f" Причина: {failure_reason}" if failure_reason else ""
                    messages.warning(request, f"Monobank відхилив списання.{suffix}")
                elif payment_status == Payment.Status.PAID:
                    messages.success(request, "Списання виконано успішно.")
                else:
                    messages.success(request, f"Списання запущено. Поточний статус: {payment_status or 'unknown'}.")
                return redirect("admin:users_telegramuser_change", object_id=user.pk)

        renewal_amount = (Decimal(settings.MONO_RENEWAL_AMOUNT) / Decimal("100")).quantize(Decimal("0.01"))
        help_text = (
            f"Це вручну запустить повне списання {renewal_amount} UAH із збереженої картки через Monobank token просто зараз. "
            "Зручно для перевірки кейсу, коли на карті недостатньо коштів: якщо банк поверне failed/rejected, "
            "ти одразу побачиш це повідомлення і причина відмови збережеться в billing profile."
        )
        context = self._object_action_context(
            request,
            user=user,
            title="Форснути списання зараз",
            form=form,
            submit_label="Запустити списання",
            help_text=help_text,
        )
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("payment_change")
    def refund_latest_payment_view(self, request, object_id):
        user = get_object_or_404(TelegramUser, pk=object_id)
        payment = get_latest_refundable_monobank_payment(user_id=user.tg_user_id)
        if payment is None:
            messages.error(request, "Немає доступного останнього Monobank списання для повного повернення.")
            return redirect("admin:users_telegramuser_change", object_id=user.pk)

        form = RefundLatestPaymentForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            before = {
                "payment_id": payment.pk,
                "invoice_id": payment.provider_payment_id,
                "payment_status": payment.status,
                "amount": str(payment.amount),
                "currency": payment.currency,
            }
            try:
                result = refund_latest_monobank_payment(
                    user_id=user.tg_user_id,
                    reason=form.cleaned_data.get("reason", ""),
                    admin_user=request.user,
                )
            except (ValueError, MonobankAPIError) as exc:
                messages.error(request, f"Не вдалося запустити refund: {exc}")
            else:
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="billing_refund_latest_payment",
                    object_type="payment",
                    object_id=payment.pk,
                    target_user_id=user.tg_user_id,
                    mode="refund_latest_payment",
                    reason=form.cleaned_data.get("reason", ""),
                    before=before,
                    after=result,
                )
                if result.get("preserved_billing_state"):
                    if result.get("refund_status") == "success":
                        messages.success(request, "Зайву 1 грн повернуто. Доступ, картка й автопродовження збережені.")
                    else:
                        messages.success(
                            request,
                            "Повернення зайвої 1 грн запущено. Доступ, картка й автопродовження не змінюються.",
                        )
                elif result.get("token_delete_error"):
                    messages.warning(
                        request,
                        "Refund запущено, але Monobank не підтвердив видалення токена. Локально автопродовження вже вимкнено.",
                    )
                elif result.get("refund_status") == "success":
                    messages.success(request, "Refund виконано. Автопродовження вимкнено.")
                else:
                    messages.success(request, "Refund запущено. Чекаємо фінальний статус від Monobank webhook.")
                return redirect("admin:users_telegramuser_change", object_id=user.pk)

        if payment.kind == Payment.Kind.BIND:
            help_text = (
                f"Буде виконано повний refund зайвого bind-платежу: {payment.amount} {payment.currency}. "
                "Система показує цю дію лише коли знаходить два однакові успішні bind-платежі протягом 24 годин. "
                "Пробний доступ, збережена картка й автопродовження залишаться без змін."
            )
            title = "Повернути зайву 1 грн"
        else:
            help_text = (
                f"Буде виконано повний refund останнього Monobank списання: {payment.amount} {payment.currency}. "
                "Для renewal/retry після запуску refund автопродовження вимикається, а збережений token видаляється."
            )
            title = "Повернути останнє списання"
        context = self._object_action_context(
            request,
            user=user,
            title=title,
            form=form,
            submit_label="Запустити refund",
            help_text=help_text,
        )
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("onboarding_reset")
    def reset_onboarding_view(self, request, object_id):
        if request.method == "POST" and request.POST.get("mode", "safe") != "safe":
            require_admin_action(request.user, "test_cleanup")
        user = get_object_or_404(TelegramUser, pk=object_id)
        form = ResetOnboardingForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            try:
                result = reset_user_onboarding(
                    user.tg_user_id,
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
            except ResetOnboardingError as exc:
                messages.error(request, str(exc))
            else:
                messages.success(request, "Онбординг скинуто. Тепер напишіть /start у Telegram.")
                return redirect("admin:users_telegramuser_change", object_id=user.pk)

        context = {
            **self.admin_site.each_context(request),
            "title": "Скинути онбординг",
            "opts": self.model._meta,
            "form": form,
            "object": user,
            "submit_label": "Скинути онбординг",
        }
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("test_cleanup")
    def erase_user_data_view(self, request, object_id):
        user = get_object_or_404(TelegramUser, pk=object_id)
        form = EraseUserDataForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            try:
                result = reset_user_onboarding(
                    user.tg_user_id,
                    "replay",
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
            except ResetOnboardingError as exc:
                messages.error(request, str(exc))
            else:
                cleanup = result["cleanup"]
                messages.success(
                    request,
                    "Дані користувача стерто. Картка і доступ збережені. "
                    f"accounts={cleanup['deleted_accounts']}, "
                    f"categories={cleanup['deleted_categories']}, "
                    f"transactions={cleanup['deleted_transactions']}.",
                )
                return redirect("admin:users_telegramuser_change", object_id=user.pk)

        context = self._object_action_context(
            request,
            user=user,
            title="Стерти дані користувача",
            form=form,
            submit_label="Стерти дані",
            help_text=(
                "Стирає рахунки, транзакції, категорії, борги, runtime-історію та онбординг, "
                "але залишає прив'язану картку, billing-профіль і поточний доступ. "
                "Дія доступна лише для тестових користувачів без family-зв'язків."
            ),
        )
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("test_cleanup")
    def hard_delete_user_view(self, request, object_id):
        user = get_object_or_404(TelegramUser, pk=object_id)
        form = HardDeleteUserForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            try:
                result = hard_delete_user(
                    user.tg_user_id,
                    admin_user=request.user,
                    reason=form.cleaned_data["reason"],
                    options={
                        "request": request,
                        "double_confirmed": form.cleaned_data["double_confirmed"],
                        "confirmation_text": form.cleaned_data["confirmation_text"],
                    },
                )
            except HardDeleteUserError as exc:
                messages.error(request, str(exc))
            else:
                cleanup = result["cleanup"]
                messages.success(
                    request,
                    "Користувача видалено з БД. "
                    f"accounts={cleanup['deleted_accounts']}, "
                    f"categories={cleanup['deleted_categories']}, "
                    f"transactions={cleanup['deleted_transactions']}, "
                    f"subscriptions={cleanup['deleted_subscriptions']}.",
                )
                return redirect("admin:users_telegramuser_changelist")

        context = self._object_action_context(
            request,
            user=user,
            title="Хардове видалення користувача",
            form=form,
            submit_label="Видалити назавжди",
            help_text=(
                "Дія безповоротно видаляє користувача з users, CRM, фінансових, billing, support, poll "
                "та runtime-таблиць. Старі audit logs цього юзера теж очищаються; залишається лише "
                "службовий запис про сам факт hard delete."
            ),
        )
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("note_add")
    def add_note_view(self, request, object_id):
        user = get_object_or_404(TelegramUser, pk=object_id)
        form = AdminNoteForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            note = AdminNote.objects.create(
                telegram_user_id=user.tg_user_id,
                admin_user=request.user,
                note_text=form.cleaned_data["note_text"],
            )
            create_audit_log(
                request=request,
                admin_user=request.user,
                action="admin_note_added",
                object_type="telegram_user",
                object_id=user.pk,
                target_user_id=user.tg_user_id,
                after={"note_id": note.pk},
            )
            messages.success(request, "Нотатку збережено.")
            return redirect("admin:users_telegramuser_change", object_id=user.pk)

        context = {
            **self.admin_site.each_context(request),
            "title": "Додати нотатку адміна",
            "opts": self.model._meta,
            "form": form,
            "object": user,
            "submit_label": "Зберегти нотатку",
        }
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("test_mark")
    def mark_test_user_view(self, request, object_id):
        user = get_object_or_404(TelegramUser, pk=object_id)
        form = TestUserForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            mark_user_as_test_user(user=user, admin_user=request.user, notes=form.cleaned_data["notes"], request=request)
            messages.success(request, "Користувача позначено як тестового.")
            return redirect("admin:users_telegramuser_change", object_id=user.pk)

        context = {
            **self.admin_site.each_context(request),
            "title": "Позначити як тестового",
            "opts": self.model._meta,
            "form": form,
            "object": user,
            "submit_label": "Позначити",
        }
        return TemplateResponse(request, "admin/object_action_form.html", context)

    @admin_action_permission("test_mark")
    def unmark_test_user_view(self, request, object_id):
        user = get_object_or_404(TelegramUser, pk=object_id)
        form = ConfirmActionForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            unmark_user_as_test_user(user=user, admin_user=request.user, request=request)
            messages.success(request, "Позначку тестового користувача знято.")
            return redirect("admin:users_telegramuser_change", object_id=user.pk)

        context = {
            **self.admin_site.each_context(request),
            "title": "Зняти позначку тестового користувача",
            "opts": self.model._meta,
            "form": form,
            "object": user,
            "submit_label": "Підтвердити",
        }
        return TemplateResponse(request, "admin/object_action_form.html", context)
