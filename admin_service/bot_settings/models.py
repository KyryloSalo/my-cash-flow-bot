from __future__ import annotations

import json

from django.conf import settings
from django.db import models


class BotSetting(models.Model):
    class ValueType(models.TextChoices):
        STRING = "string", "String"
        INT = "int", "Integer"
        BOOL = "bool", "Boolean"
        JSON = "json", "JSON"

    key = models.CharField(max_length=128, unique=True)
    value = models.TextField(blank=True, default="")
    description = models.TextField(blank=True, default="")
    value_type = models.CharField(max_length=16, choices=ValueType.choices, default=ValueType.STRING)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="updated_bot_settings",
    )
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "bot_settings"
        verbose_name = "Bot Setting"
        verbose_name_plural = "Bot Settings"
        ordering = ("key",)

    def parsed_value(self):
        if self.value_type == self.ValueType.INT:
            try:
                return int(self.value)
            except (TypeError, ValueError):
                return 0
        if self.value_type == self.ValueType.BOOL:
            return str(self.value).strip().lower() in {"1", "true", "yes", "on"}
        if self.value_type == self.ValueType.JSON:
            try:
                return json.loads(self.value or "{}")
            except json.JSONDecodeError:
                return {}
        return self.value

    def __str__(self) -> str:
        return self.key

