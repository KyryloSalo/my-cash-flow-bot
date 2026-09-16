from __future__ import annotations

from django.db import models
from django.utils import timezone

from users.models import TelegramUser


class Account(models.Model):
    id = models.BigAutoField(primary_key=True)
    tg_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.DO_NOTHING,
        db_column="tg_user_id",
        to_field="tg_user_id",
        related_name="bot_accounts",
    )
    family_id = models.BigIntegerField(blank=True, null=True)
    label = models.TextField()
    currency = models.TextField()
    account_type = models.TextField(default="other")
    starting_balance = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    balance = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    credit_limit = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    monthly_interest_rate = models.DecimalField(max_digits=18, decimal_places=4, blank=True, null=True)
    non_negative_account_type = models.TextField(default="main")
    goal_name = models.TextField(blank=True, null=True)
    goal_amount = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    goal_date = models.DateField(blank=True, null=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "accounts"
        verbose_name = "Рахунок"
        verbose_name_plural = "Рахунки"
        ordering = ("label",)

    def __str__(self) -> str:
        return f"{self.label} ({self.currency})"

    @property
    def name(self) -> str:
        return self.label

    def save(self, *args, **kwargs):
        now = timezone.now()
        if not self.created_at:
            self.created_at = now
        self.updated_at = now
        if self.balance is None:
            self.balance = self.starting_balance or 0
        if not self.account_type:
            self.account_type = "other"
        if not self.non_negative_account_type:
            self.non_negative_account_type = "main"
        super().save(*args, **kwargs)


class AccountAdminState(models.Model):
    account = models.OneToOneField(Account, on_delete=models.CASCADE, related_name="admin_state")
    is_default = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "account_admin_states"
        verbose_name = "Стан рахунку"
        verbose_name_plural = "Стани рахунків"

    def __str__(self) -> str:
        return f"{self.account_id}"
