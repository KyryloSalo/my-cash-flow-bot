from __future__ import annotations

from django import forms
from django.contrib import admin
from django.db import models
from django.urls import reverse
from django.utils.html import format_html

from bot_settings.models import BotSetting
from common.admin import AuditedModelAdmin
from common.admin_site import admin_site
from common.admin_ui import compact_text


BotSetting._meta.verbose_name = "Налаштування бота"
BotSetting._meta.verbose_name_plural = "Налаштування бота"

SETTING_GROUPS = {
    "base_currency": "Підписки та оплата",
    "trial_duration_days": "Підписки та оплата",
    "payment_link": "Підписки та оплата",
    "payment_link_trial_30": "Підписки та оплата",
    "payment_link_trial_90": "Підписки та оплата",
    "registration_enabled": "Доступ",
    "maintenance_mode": "Доступ",
    "maintenance_message": "Доступ",
    "support_admin_telegram_id": "Підтримка",
    "support_contact_text": "Підтримка",
    "support_intro_text": "Тексти",
    "default_subscription_expiring_text": "Тексти",
    "default_subscription_expired_text": "Тексти",
    "reset_onboarding_notice_text": "Тексти",
    "onboarding_welcome_text": "Тексти",
    "onboarding_restart_notice_text": "Тексти",
    "onboarding_currency_step_text": "Тексти",
    "onboarding_start_date_step_text": "Тексти",
    "onboarding_categories_step_text": "Тексти",
    "onboarding_categories_custom_text": "Тексти",
    "settings_intro_text": "Тексти",
    "reports_intro_text": "Тексти",
    "export_intro_text": "Тексти",
    "categories_intro_text": "Тексти",
    "family_disabled_text": "Тексти",
    "family_empty_text": "Тексти",
    "accounts_empty_text": "Тексти",
    "savings_empty_text": "Тексти",
    "savings_missing_account_text": "Тексти",
    "savings_need_first_account_text": "Тексти",
    "hidden_categories_empty_text": "Тексти",
    "saving_reminder_text": "Тексти",
    "saving_after_income_text": "Тексти",
    "saving_setup_text": "Тексти",
    "saving_plan_text": "Тексти",
    "saving_topup_confirm_text": "Тексти",
    "saving_post_income_confirm_text": "Тексти",
    "debt_invite_post_create_text": "Тексти",
    "debt_invite_owner_message_text": "Тексти",
    "debt_invite_share_message_text": "Тексти",
    "debt_invite_claim_text": "Тексти",
    "debt_monthly_reminder_text": "Тексти",
    "poll_low_rating_threshold": "Технічне",
}

SETTING_HINTS = {
    "payment_link": "Базове посилання на оплату.",
    "payment_link_trial_30": "Окреме посилання для сценарію з 30 безкоштовними днями.",
    "payment_link_trial_90": "Окреме посилання для сценарію з 90 безкоштовними днями.",
    "default_subscription_expiring_text": "Текст нагадування перед завершенням підписки.",
    "default_subscription_expired_text": "Текст після завершення підписки.",
    "reset_onboarding_notice_text": "Повідомлення користувачу після скидання онбордингу.",
    "support_intro_text": "Стартовий текст звернення в підтримку.",
    "onboarding_welcome_text": "Привітання і перший крок онбордингу.",
    "onboarding_restart_notice_text": "Пояснення перед повторним запуском онбордингу.",
    "onboarding_currency_step_text": "Пояснення на кроці вибору базової валюти.",
    "onboarding_start_date_step_text": "Пояснення на кроці вибору стартової дати.",
    "onboarding_categories_step_text": "Пояснення на кроці вибору режиму категорій.",
    "onboarding_categories_custom_text": "Пояснення на екрані ручного налаштування категорій.",
    "settings_intro_text": "Заголовок і короткий текст екрана налаштувань.",
    "reports_intro_text": "Заголовок і короткий текст екрана звітів.",
    "export_intro_text": "Заголовок і короткий текст екрана експорту.",
    "categories_intro_text": "Пояснення на вході в меню категорій.",
    "family_disabled_text": "Повідомлення, коли сімейний режим тимчасово вимкнений.",
    "family_empty_text": "Порожній стан сімейного режиму для користувача без родини.",
    "accounts_empty_text": "Порожній стан списку рахунків.",
    "savings_empty_text": "Порожній стан заощаджень та інвестицій.",
    "savings_missing_account_text": "Повідомлення, коли рахунок накопичень не знайдено.",
    "savings_need_first_account_text": "Повідомлення, коли треба спершу створити рахунок накопичень.",
    "hidden_categories_empty_text": "Повідомлення, коли немає прихованих категорій.",
    "saving_reminder_text": "Текст автоматичного нагадування про відкладення.",
    "saving_after_income_text": "Короткий nudge після збереження доходу.",
    "saving_setup_text": "Nudge після доходу, коли ще немає жодного накопичення.",
    "saving_plan_text": "Текст створеного плану відкладення.",
    "saving_topup_confirm_text": "Пояснення перед записом поповнення накопичення.",
    "saving_post_income_confirm_text": "Пояснення перед записом відкладення після доходу.",
    "debt_invite_post_create_text": "Текст після збереження боргу з пропозицією увімкнути нагадування.",
    "debt_invite_owner_message_text": "Текст, який власник боргу надсилає боржнику.",
    "debt_invite_share_message_text": "Текст для Telegram share боргу.",
    "debt_invite_claim_text": "Текст екрана підтвердження боргу для боржника.",
    "debt_monthly_reminder_text": "Текст щомісячного автоматичного нагадування про борг.",
    "poll_low_rating_threshold": "Поріг низької оцінки для внутрішніх тригерів.",
}


class BotSettingGroupListFilter(admin.SimpleListFilter):
    title = "група"
    parameter_name = "setting_group"

    def lookups(self, request, model_admin):
        groups = sorted(set(SETTING_GROUPS.values()))
        return [(group, group) for group in groups]

    def queryset(self, request, queryset):
        if not self.value():
            return queryset
        keys = [key for key, group in SETTING_GROUPS.items() if group == self.value()]
        if not keys:
            return queryset.none()
        return queryset.filter(key__in=keys)


@admin.register(BotSetting, site=admin_site)
class BotSettingAdmin(AuditedModelAdmin):
    audit_object_type = "bot_setting"
    list_display = ("group_display", "key", "short_value", "type_display", "updated_at", "updated_by", "edit_link")
    list_display_links = ("key", "short_value")
    search_fields = ("key", "description", "value")
    list_filter = (BotSettingGroupListFilter, "value_type", "updated_at")
    readonly_fields = ("updated_at", "updated_by", "parsed_value_preview", "group_display")
    ordering = ("key",)
    actions = None
    formfield_overrides = {
        models.TextField: {
            "widget": forms.Textarea(
                attrs={
                    "rows": 6,
                    "style": "min-width: 36rem;",
                }
            )
        }
    }
    fieldsets = (
        (
            "Ключ",
            {
                "fields": ("key", "group_display", "value_type", "description"),
            },
        ),
        (
            "Значення",
            {
                "fields": ("value", "parsed_value_preview"),
            },
        ),
        (
            "Службове",
            {
                "fields": ("updated_at", "updated_by"),
            },
        ),
    )

    def get_readonly_fields(self, request, obj=None):
        fields = list(super().get_readonly_fields(request, obj))
        if obj is not None:
            fields.extend(["key", "value_type"])
        return fields

    def has_add_permission(self, request):
        return False

    @admin.display(description="Група")
    def group_display(self, obj: BotSetting):
        if obj is None:
            return "-"
        return SETTING_GROUPS.get(obj.key, "Інше")

    @admin.display(description="Значення")
    def short_value(self, obj: BotSetting):
        return compact_text(obj.value, limit=84)

    @admin.display(description="Тип", ordering="value_type")
    def type_display(self, obj: BotSetting):
        labels = {"str": "Текст", "string": "Текст", "int": "Число", "integer": "Число", "bool": "Так / ні", "boolean": "Так / ні", "json": "JSON"}
        return labels.get(obj.value_type, obj.value_type or "—")

    @admin.display(description="Дія")
    def edit_link(self, obj: BotSetting):
        return format_html('<a class="op-row-action" href="{}">Редагувати</a>', reverse("admin:bot_settings_botsetting_change", args=[obj.pk]))

    @admin.display(description="Розібране значення")
    def parsed_value_preview(self, obj: BotSetting):
        if obj is None:
            return "-"
        parsed = obj.parsed_value()
        hint = SETTING_HINTS.get(obj.key, "")
        if hint:
            return f"{parsed}\n\n{hint}"
        return parsed

    def save_model(self, request, obj, form, change):
        obj.updated_by = request.user
        super().save_model(request, obj, form, change)
