from __future__ import annotations

from django import forms
from django.contrib import admin, messages
from django.urls import reverse

from accounts.models import Account, AccountAdminState
from common.admin import AuditedModelAdmin, HiddenFromMenuAdminMixin, PrivateFinancialDataAdminMixin
from common.admin_actions import bulk_action_confirmation
from common.admin_site import admin_site
from common.admin_ui import boolean_badge, money_value, user_identity

Account._meta.verbose_name = "Рахунок"
Account._meta.verbose_name_plural = "Рахунки"


class AccountAdminForm(forms.ModelForm):
    is_default = forms.BooleanField(required=False, label="Рахунок за замовчуванням")

    class Meta:
        model = Account
        fields = ("tg_user", "label", "currency", "account_type", "starting_balance", "balance", "is_active")
        labels = {
            "tg_user": "Користувач",
            "label": "Назва рахунку",
            "currency": "Валюта",
            "account_type": "Тип рахунку",
            "starting_balance": "Початковий баланс",
            "balance": "Баланс",
            "is_active": "Активний",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            state = getattr(self.instance, "admin_state", None)
            self.fields["is_default"].initial = bool(state and state.is_default)


@admin.register(Account, site=admin_site)
class AccountAdmin(PrivateFinancialDataAdminMixin, AuditedModelAdmin):
    audit_object_type = "account"
    form = AccountAdminForm
    list_display = (
        "user_display",
        "account_name",
        "balance_display",
        "account_type_value",
        "currency",
        "default_display",
        "active_display",
        "updated_at",
    )
    list_display_links = ("account_name",)
    search_fields = ("label", "tg_user__username", "tg_user__tg_user_id")
    list_filter = ("currency", "account_type", "is_active", "created_at", ("tg_user__admin_state__is_test_user", admin.BooleanFieldListFilter))
    readonly_fields = ("balance", "created_at", "updated_at")
    fields = ("tg_user", "label", "currency", "account_type", "starting_balance", "balance", "is_default", "is_active", "created_at", "updated_at")

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("tg_user", "tg_user__admin_state").prefetch_related("admin_state")

    def get_readonly_fields(self, request, obj=None):
        fields = list(super().get_readonly_fields(request, obj))
        if obj is not None:
            fields.extend(["tg_user", "currency", "account_type", "starting_balance"])
        return fields

    @admin.display(description="Користувач", ordering="tg_user__username")
    def user_display(self, obj: Account):
        return user_identity(obj.tg_user)

    @admin.display(description="Рахунок", ordering="label")
    def account_name(self, obj: Account):
        return obj.label

    @admin.display(description="Баланс", ordering="balance")
    def balance_display(self, obj: Account):
        return money_value(obj.balance, obj.currency)

    @admin.display(description="Тип", ordering="account_type")
    def account_type_value(self, obj: Account):
        labels = {
            "main": "Основний",
            "cash": "Готівка",
            "savings": "Заощадження",
            "deposit": "Депозит",
            "investment": "Інвестиції",
            "credit": "Кредитний",
            "other": "Інший",
        }
        return labels.get(obj.account_type, obj.account_type or "—")

    @admin.display(description="За замовчуванням", ordering="admin_state__is_default")
    def default_display(self, obj: Account):
        state = getattr(obj, "admin_state", None)
        return boolean_badge(bool(state and state.is_default))

    @admin.display(description="Стан", ordering="is_active")
    def active_display(self, obj: Account):
        state = getattr(obj, "admin_state", None)
        is_active = bool(obj.is_active and (state is None or state.is_active))
        return boolean_badge(is_active, true_label="Активний", false_label="Неактивний")

    @admin.action(description="Зробити рахунком за замовчуванням")
    def make_default(self, request, queryset):
        if request.POST.get("confirm_action") != "1":
            return bulk_action_confirmation(
                modeladmin=self,
                request=request,
                queryset=queryset,
                title="Змінити рахунок за замовчуванням?",
                warning=(
                    "Для кожної пари користувач + валюта вибраний рахунок стане основним, "
                    "а попередній основний рахунок втратить цю ознаку."
                ),
                submit_label="Змінити основні рахунки",
                section="Фінанси · Рахунки",
                cancel_url=reverse("admin:accounts_account_changelist"),
            )
        for account in queryset.select_related("tg_user"):
            AccountAdminState.objects.update_or_create(
                account=account,
                defaults={"is_default": True, "is_active": account.is_active},
            )
            AccountAdminState.objects.filter(
                account__tg_user_id=account.tg_user_id,
                account__currency=account.currency,
            ).exclude(account_id=account.pk).update(is_default=False)
        messages.success(request, "Основні рахунки оновлено.")

    @admin.action(description="Деактивувати рахунок")
    def deactivate_accounts(self, request, queryset):
        if request.POST.get("confirm_action") != "1":
            return bulk_action_confirmation(
                modeladmin=self,
                request=request,
                queryset=queryset,
                title="Деактивувати вибрані рахунки?",
                warning="Рахунки збережуть баланс та історію, але перестануть бути доступними для нових операцій.",
                submit_label="Деактивувати рахунки",
                section="Фінанси · Рахунки",
                cancel_url=reverse("admin:accounts_account_changelist"),
            )
        updated = queryset.update(is_active=False)
        AccountAdminState.objects.filter(account__in=queryset).update(is_active=False, is_default=False)
        messages.success(request, f"Деактивовано рахунків: {updated}.")

    actions = ("make_default", "deactivate_accounts")

    def save_model(self, request, obj, form, change):
        if obj.balance is None:
            obj.balance = obj.starting_balance
        super().save_model(request, obj, form, change)
        state, _ = AccountAdminState.objects.get_or_create(account=obj)
        state.is_default = form.cleaned_data.get("is_default", False)
        state.is_active = obj.is_active
        state.save()
        if state.is_default:
            AccountAdminState.objects.filter(
                account__tg_user_id=obj.tg_user_id,
                account__currency=obj.currency,
            ).exclude(account_id=obj.pk).update(is_default=False)

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False


@admin.register(AccountAdminState, site=admin_site)
class AccountAdminStateAdmin(PrivateFinancialDataAdminMixin, HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "account_admin_state"
    list_display = ("account", "is_default", "is_active", "updated_at")
    search_fields = ("account__label", "account__tg_user__username", "account__tg_user__tg_user_id")
    list_filter = ("is_default", "is_active", "updated_at")
    readonly_fields = ("updated_at",)
