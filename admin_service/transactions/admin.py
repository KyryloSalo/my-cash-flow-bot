from __future__ import annotations

from decimal import Decimal

from django import forms
from django.contrib import admin
from django.db import transaction
from django.utils.html import format_html

from accounts.models import Account
from common.admin import AuditedModelAdmin, PrivateFinancialDataAdminMixin
from common.admin_site import admin_site
from common.admin_ui import badge, compact_text, money_value, status_badge, user_identity
from transactions.models import Debt, DebtPayment, Transaction

Transaction._meta.verbose_name = "Транзакція"
Transaction._meta.verbose_name_plural = "Транзакції"
Debt._meta.verbose_name = "Борг"
Debt._meta.verbose_name_plural = "Борги"
DebtPayment._meta.verbose_name = "Платіж за боргом"
DebtPayment._meta.verbose_name_plural = "Платежі за боргами"


ZERO = Decimal("0.00")


class TransactionAdminForm(forms.ModelForm):
    TYPE_CHOICES = (
        ("expense", "expense"),
        ("income", "income"),
        ("transfer", "transfer"),
        ("debt_given", "debt_given"),
        ("debt_received", "debt_received"),
        ("debt_repayment_in", "debt_repayment_in"),
        ("debt_repayment_out", "debt_repayment_out"),
    )

    type = forms.ChoiceField(choices=TYPE_CHOICES, label="Тип")

    class Meta:
        model = Transaction
        fields = (
            "tg_user",
            "date",
            "type",
            "amount",
            "currency",
            "category",
            "account",
            "from_account",
            "to_account",
            "to_amount",
            "to_currency",
            "comment",
        )
        labels = {
            "tg_user": "Користувач",
            "date": "Дата",
            "amount": "Сума",
            "currency": "Валюта",
            "category": "Категорія",
            "account": "Рахунок",
            "from_account": "Рахунок-джерело",
            "to_account": "Рахунок-отримувач",
            "to_amount": "Сума зарахування",
            "to_currency": "Валюта зарахування",
            "comment": "Коментар",
        }

    def clean(self):
        cleaned = super().clean()
        tx_type = cleaned.get("type")
        account = cleaned.get("account")
        from_account = cleaned.get("from_account")
        to_account = cleaned.get("to_account")

        if tx_type in {"expense", "income"} and not account:
            self.add_error("account", "Для income/expense потрібен рахунок.")
        if tx_type in {"debt_given", "debt_received", "debt_repayment_in", "debt_repayment_out"} and not account:
            self.add_error("account", "Для debt потрібен рахунок.")
        if tx_type == "transfer":
            if not from_account:
                self.add_error("from_account", "Для transfer потрібен рахунок-джерело.")
            if not to_account:
                self.add_error("to_account", "Для transfer потрібен рахунок-приймач.")
            if from_account and to_account and from_account.pk == to_account.pk:
                self.add_error("to_account", "Рахунки transfer мають бути різними.")
        return cleaned


def _apply_account_delta(account: Account | None, delta: Decimal) -> None:
    if not account:
        return
    account.balance = (account.balance or ZERO) + delta
    account.save(update_fields=["balance", "updated_at"])


def _apply_transaction_effect(tx: Transaction, reverse: bool = False) -> None:
    factor = Decimal("-1") if reverse else Decimal("1")
    amount = tx.amount or ZERO
    to_amount = tx.to_amount or amount

    if tx.type == "expense":
        _apply_account_delta(tx.account, factor * -amount)
    elif tx.type == "income":
        _apply_account_delta(tx.account, factor * amount)
    elif tx.type == "transfer":
        _apply_account_delta(tx.from_account, factor * -amount)
        _apply_account_delta(tx.to_account, factor * to_amount)
    elif tx.type == "debt_given":
        _apply_account_delta(tx.account, factor * -amount)
    elif tx.type == "debt_received":
        _apply_account_delta(tx.account, factor * amount)
    elif tx.type == "debt_repayment_in":
        _apply_account_delta(tx.account, factor * amount)
    elif tx.type == "debt_repayment_out":
        _apply_account_delta(tx.account, factor * -amount)


@admin.register(Debt, site=admin_site)
class DebtAdmin(PrivateFinancialDataAdminMixin, AuditedModelAdmin):
    audit_object_type = "debt"
    list_display = (
        "user_display",
        "counterparty_name",
        "direction_display",
        "remaining_display",
        "progress_display",
        "due_date",
        "status_display",
        "updated_at",
    )
    list_display_links = ("counterparty_name",)
    list_filter = ("direction", "status", "currency", "created_at")
    search_fields = ("counterparty_name", "comment", "tg_user__username", "tg_user__tg_user_id")
    readonly_fields = ("created_at", "updated_at", "closed_at")

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("tg_user", "account")

    @admin.display(description="Користувач", ordering="tg_user__username")
    def user_display(self, obj: Debt):
        return user_identity(obj.tg_user)

    @admin.display(description="Напрям", ordering="direction")
    def direction_display(self, obj: Debt):
        if obj.direction == "receivable":
            return badge("Мені винні", tone="positive")
        if obj.direction == "payable":
            return badge("Я винен", tone="warning")
        return badge(obj.direction or "Невідомо", tone="info")

    @admin.display(description="Залишок", ordering="remaining_amount")
    def remaining_display(self, obj: Debt):
        return money_value(obj.remaining_amount, obj.currency)

    @admin.display(description="Погашено")
    def progress_display(self, obj: Debt):
        initial = obj.initial_amount or ZERO
        paid = obj.paid_amount or ZERO
        percent = min(100, max(0, int((paid / initial) * 100))) if initial else 0
        return format_html(
            '<span class="op-progress"><span><i style="width:{}%"></i></span><small>{}%</small></span>',
            percent,
            percent,
        )

    @admin.display(description="Статус", ordering="status")
    def status_display(self, obj: Debt):
        labels = {"active": "Активний", "open": "Відкритий", "closed": "Закритий", "completed": "Закритий"}
        return status_badge(obj.status, labels.get(obj.status, obj.status or "Невідомо"))


@admin.register(DebtPayment, site=admin_site)
class DebtPaymentAdmin(PrivateFinancialDataAdminMixin, AuditedModelAdmin):
    audit_object_type = "debt_payment"
    list_display = ("user_display", "debt", "amount_display", "account", "payment_date", "comment_display", "created_at")
    list_display_links = ("debt",)
    list_filter = ("currency", "payment_date", "created_at")
    search_fields = ("debt__counterparty_name", "comment", "tg_user__username", "tg_user__tg_user_id")
    readonly_fields = ("created_at",)

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("tg_user", "debt", "account")

    @admin.display(description="Користувач", ordering="tg_user__username")
    def user_display(self, obj: DebtPayment):
        return user_identity(obj.tg_user)

    @admin.display(description="Сума", ordering="amount")
    def amount_display(self, obj: DebtPayment):
        return money_value(obj.amount, obj.currency)

    @admin.display(description="Коментар")
    def comment_display(self, obj: DebtPayment):
        return compact_text(obj.comment)


@admin.register(Transaction, site=admin_site)
class TransactionAdmin(PrivateFinancialDataAdminMixin, AuditedModelAdmin):
    audit_object_type = "transaction"
    form = TransactionAdminForm
    list_display = (
        "date",
        "user_display",
        "type_display",
        "amount_display",
        "account_display",
        "category",
        "comment_display",
        "source_display",
    )
    list_display_links = ("date",)
    search_fields = ("comment", "source", "tg_user__username", "tg_user__tg_user_id")
    list_filter = ("type", "currency", "flow_kind", "created_at", "date", ("tg_user__admin_state__is_test_user", admin.BooleanFieldListFilter))
    readonly_fields = (
        "source",
        "source_type",
        "created_at",
        "fx_rate",
        "fx_rate_text",
        "flow_kind",
        "counterparty",
        "debt_action",
        "debt",
        "debt_payment",
        "original_amount",
        "original_currency",
        "exchange_rate",
    )
    fields = (
        "tg_user",
        "date",
        "type",
        "amount",
        "currency",
        "category",
        "account",
        "from_account",
        "to_account",
        "to_amount",
        "to_currency",
        "comment",
        "source",
        "source_type",
        "fx_rate",
        "fx_rate_text",
        "flow_kind",
        "counterparty",
        "debt_action",
        "debt",
        "debt_payment",
        "original_amount",
        "original_currency",
        "exchange_rate",
        "created_at",
    )

    @transaction.atomic
    def save_model(self, request, obj, form, change):
        original = self.model.objects.select_related("account", "from_account", "to_account").filter(pk=obj.pk).first() if change else None
        if original is not None:
            _apply_transaction_effect(original, reverse=True)
        obj.source = "manual_admin"
        super().save_model(request, obj, form, change)
        obj.refresh_from_db()
        _apply_transaction_effect(obj, reverse=False)

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("tg_user", "tg_user__admin_state", "category", "account", "from_account", "to_account")

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    @admin.display(description="Користувач", ordering="tg_user__username")
    def user_display(self, obj: Transaction):
        return user_identity(obj.tg_user)

    @admin.display(description="Тип", ordering="type")
    def type_display(self, obj: Transaction):
        labels = {
            "expense": ("Витрата", "danger"),
            "income": ("Дохід", "positive"),
            "transfer": ("Переказ", "info"),
            "debt_given": ("Дав у борг", "warning"),
            "debt_received": ("Взяв у борг", "warning"),
            "debt_repayment_in": ("Повернули борг", "positive"),
            "debt_repayment_out": ("Повернув борг", "danger"),
        }
        label, tone = labels.get(obj.type, (obj.type or "Невідомо", "info"))
        if obj.flow_kind == "adjustment":
            label, tone = "Коригування", "warning"
        return badge(label, tone=tone)

    @admin.display(description="Сума", ordering="amount")
    def amount_display(self, obj: Transaction):
        return money_value(obj.amount, obj.currency)

    @admin.display(description="Рахунок")
    def account_display(self, obj: Transaction):
        if obj.type == "transfer" or obj.flow_kind == "transfer":
            source = getattr(obj.from_account, "label", None) or "—"
            target = getattr(obj.to_account, "label", None) or "—"
            return f"{source} → {target}"
        return getattr(obj.account, "label", None) or "—"

    @admin.display(description="Коментар")
    def comment_display(self, obj: Transaction):
        return compact_text(obj.comment)

    @admin.display(description="Джерело", ordering="source")
    def source_display(self, obj: Transaction):
        labels = {"voice": "Голос", "manual": "Вручну", "button": "Кнопка", "text": "Текст"}
        return labels.get(obj.source_type, obj.source_type or "—")

    @admin.display(boolean=True, description="Тестовий користувач")
    def is_test_user(self, obj: Transaction):
        state = getattr(obj.tg_user, "admin_state", None)
        return bool(state and state.is_test_user)
