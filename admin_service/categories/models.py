from __future__ import annotations

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.utils import timezone

from users.models import TelegramUser


class CategoryTemplate(models.Model):
    id = models.BigAutoField(primary_key=True)
    type = models.TextField()
    name = models.TextField()
    slug = models.TextField(blank=True, null=True)
    aliases = ArrayField(models.TextField(), default=list, blank=True)
    sort_order = models.IntegerField(default=0)
    is_system = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "category_templates"
        verbose_name = "Шаблон категорії"
        verbose_name_plural = "Шаблони категорій"
        ordering = ("type", "sort_order", "name")

    def __str__(self) -> str:
        return f"{self.type}: {self.name}"

    def save(self, *args, **kwargs):
        now = timezone.now()
        if not self.created_at:
            self.created_at = now
        self.updated_at = now
        super().save(*args, **kwargs)


class Category(models.Model):
    id = models.BigAutoField(primary_key=True)
    tg_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.DO_NOTHING,
        db_column="tg_user_id",
        to_field="tg_user_id",
        blank=True,
        null=True,
        related_name="legacy_categories",
    )
    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.DO_NOTHING,
        db_column="user_id",
        to_field="tg_user_id",
        blank=True,
        null=True,
        related_name="categories",
    )
    family_id = models.BigIntegerField(blank=True, null=True)
    created_by_user_id = models.BigIntegerField(blank=True, null=True)
    template = models.ForeignKey(
        CategoryTemplate,
        on_delete=models.SET_NULL,
        db_column="template_id",
        blank=True,
        null=True,
        related_name="categories",
    )
    kind = models.TextField(blank=True, null=True)
    type = models.TextField(blank=True, null=True)
    name = models.TextField()
    slug = models.TextField(blank=True, null=True)
    aliases = ArrayField(models.TextField(), default=list, blank=True)
    source = models.TextField(default="legacy")
    is_system = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    sort_order = models.IntegerField(default=0)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    deleted_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "categories"
        verbose_name = "Категорія"
        verbose_name_plural = "Категорії"
        ordering = ("-is_system", "sort_order", "name")

    def __str__(self) -> str:
        return self.name

    @property
    def owner(self):
        return self.user or self.tg_user

    @property
    def category_type(self) -> str:
        return self.type or self.kind or ""

    def save(self, *args, **kwargs):
        now = timezone.now()
        owner_id = self.user_id or self.tg_user_id
        if owner_id:
            self.user_id = owner_id
            self.tg_user_id = owner_id
            self.is_system = False
        else:
            self.user_id = None
            self.tg_user_id = None
            self.is_system = True

        if self.type and not self.kind:
            self.kind = self.type
        if self.kind and not self.type:
            self.type = self.kind
        if not self.source:
            self.source = "system" if self.is_system else "manual_admin"
        if not self.created_at:
            self.created_at = now
        self.updated_at = now
        super().save(*args, **kwargs)
