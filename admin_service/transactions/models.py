from __future__ import annotations

from django.db import models
from django.utils import timezone

from accounts.models import Account
from categories.models import Category
from users.models import TelegramUser


class ActiveTransactionManager(models.Manager):
    def get_queryset(self):
        return (
            super()
            .get_queryset()
            .filter(deleted_at__isnull=True)
            .filter(models.Q(is_deleted=False) | models.Q(is_deleted__isnull=True))
        )


class Debt(models.Model):
    id = models.BigAutoField(primary_key=True)
    tg_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.DO_NOTHING,
        db_column="tg_user_id",
        to_field="tg_user_id",
        related_name="debts",
    )
    family_id = models.BigIntegerField(blank=True, null=True)
    counterparty_name = models.TextField()
    direction = models.TextField()
    initial_amount = models.DecimalField(max_digits=18, decimal_places=2)
    paid_amount = models.DecimalField(max_digits=18, decimal_places=2)
    remaining_amount = models.DecimalField(max_digits=18, decimal_places=2)
    currency = models.TextField()
    account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        db_column="account_id",
        related_name="debts",
    )
    status = models.TextField()
    due_date = models.DateField(blank=True, null=True)
    comment = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()
    closed_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "debts"
        verbose_name = "Debt"
        verbose_name_plural = "Debts"
        ordering = ("-created_at", "-id")

    def __str__(self) -> str:
        return f"{self.tg_user_id} {self.counterparty_name} {self.direction} {self.remaining_amount} {self.currency}"


class DebtPayment(models.Model):
    id = models.BigAutoField(primary_key=True)
    tg_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.DO_NOTHING,
        db_column="tg_user_id",
        to_field="tg_user_id",
        related_name="debt_payments",
    )
    family_id = models.BigIntegerField(blank=True, null=True)
    debt = models.ForeignKey(
        Debt,
        on_delete=models.CASCADE,
        db_column="debt_id",
        related_name="payments",
    )
    amount = models.DecimalField(max_digits=18, decimal_places=2)
    currency = models.TextField()
    account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        db_column="account_id",
        related_name="debt_payments",
    )
    payment_date = models.DateField()
    comment = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = "debt_payments"
        verbose_name = "Debt payment"
        verbose_name_plural = "Debt payments"
        ordering = ("-created_at", "-id")

    def __str__(self) -> str:
        return f"{self.tg_user_id} debt={self.debt_id} {self.amount} {self.currency}"


class Transaction(models.Model):
    id = models.BigAutoField(primary_key=True)
    tg_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.DO_NOTHING,
        db_column="tg_user_id",
        to_field="tg_user_id",
        related_name="transactions",
    )
    family_id = models.BigIntegerField(blank=True, null=True)
    date = models.DateField()
    type = models.TextField()
    amount = models.DecimalField(max_digits=18, decimal_places=2)
    currency = models.TextField()
    to_amount = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    to_currency = models.TextField(blank=True, null=True)
    fx_rate = models.DecimalField(max_digits=18, decimal_places=6, blank=True, null=True)
    fx_rate_text = models.TextField(blank=True, null=True)
    fx_rate_source = models.TextField(blank=True, null=True)
    comment = models.TextField(blank=True, null=True)
    source = models.TextField()
    account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        db_column="account_id",
        related_name="legacy_account_transactions",
    )
    category = models.ForeignKey(
        Category,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        db_column="category_id",
        related_name="transactions",
    )
    from_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        db_column="from_account_id",
        related_name="outgoing_transfers",
    )
    to_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        db_column="to_account_id",
        related_name="incoming_transfers",
    )
    flow_kind = models.TextField(default="normal")
    transfer_subtype = models.TextField(blank=True, null=True)
    counterparty = models.TextField(blank=True, null=True)
    debt_action = models.TextField(blank=True, null=True)
    debt = models.ForeignKey(
        Debt,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        db_column="debt_id",
        related_name="transactions",
    )
    debt_payment = models.ForeignKey(
        DebtPayment,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        db_column="debt_payment_id",
        related_name="transactions",
    )
    original_amount = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    original_currency = models.TextField(blank=True, null=True)
    exchange_rate = models.DecimalField(max_digits=18, decimal_places=6, blank=True, null=True)
    category_name_snapshot = models.TextField(blank=True, null=True)
    created_by_user_id = models.BigIntegerField(blank=True, null=True)
    updated_by_user_id = models.BigIntegerField(blank=True, null=True)
    is_deleted = models.BooleanField(default=False, blank=True, null=True)
    deleted_at = models.DateTimeField(blank=True, null=True)
    deleted_by_user_id = models.BigIntegerField(blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)

    objects = ActiveTransactionManager()
    all_objects = models.Manager()

    class Meta:
        managed = False
        db_table = "transactions"
        verbose_name = "Транзакція"
        verbose_name_plural = "Транзакції"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.tg_user_id} {self.type} {self.amount} {self.currency}"

    @property
    def source_type(self) -> str:
        value = (self.source or "").lower()
        if "voice" in value:
            return "voice"
        if "manual" in value:
            return "manual"
        if "button" in value:
            return "button"
        return "text"

    def save(self, *args, **kwargs):
        if not self.created_at:
            self.created_at = timezone.now()
        if not self.source:
            self.source = "manual_admin"
        if self.type == "transfer":
            if self.to_amount is None:
                self.to_amount = self.amount
            if not self.to_currency:
                self.to_currency = self.currency
        super().save(*args, **kwargs)
