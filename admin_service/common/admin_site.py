from __future__ import annotations

from copy import deepcopy

from django.conf import settings
from django.contrib import admin
from django.contrib.auth import get_user_model
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin
from django.template.response import TemplateResponse
from django.urls import path, reverse
from unfold.sites import UnfoldAdminSite

from common.admin import ADMIN_PAGE_METADATA
from common.admin_actions import can_admin_action
from common.admin_navigation import PRIMARY_MENU_SECTIONS, TECHNICAL_MENU_SECTIONS
from common.admin_pages import (
    admin_notifications_view,
    can_use_test_tools,
    healthcheck_view,
    manual_message_view,
    my_test_user_view,
    onboarding_debug_view,
    qa_tools_view,
    reset_my_onboarding_view,
)
from common.admin_ui import boolean_badge


class CashFlowAdminSite(UnfoldAdminSite):
    site_header = "Vydno Control"
    site_title = "Vydno Control"
    index_title = "\u041e\u0433\u043b\u044f\u0434"
    index_template = "dashboard/index.html"

    menu_structure = PRIMARY_MENU_SECTIONS
    technical_menu_structure = TECHNICAL_MENU_SECTIONS

    def index(self, request, extra_context=None):
        from dashboard.services import build_dashboard_context

        context = {
            **self.each_context(request),
            **build_dashboard_context(request),
            "app_list": self.get_app_list(request),
        }
        if extra_context:
            context.update(extra_context)
        request.current_app = self.name
        return TemplateResponse(request, self.index_template, context)

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("qa-tools/", self.admin_view(self.qa_tools_view), name="qa_tools"),
            path("manual-message/", self.admin_view(self.manual_message_view), name="manual_message"),
            path("my-test-user/", self.admin_view(self.my_test_user_view), name="my_test_user"),
            path("reset-my-onboarding/", self.admin_view(self.reset_my_onboarding_view), name="reset_my_onboarding"),
            path("onboarding-debug/", self.admin_view(self.onboarding_debug_view), name="onboarding_debug"),
            path("healthcheck/", self.admin_view(self.healthcheck_view), name="system_health"),
            path("admin-notifications/", self.admin_view(self.admin_notifications_view), name="admin_notifications"),
        ]
        return custom_urls + urls

    def qa_tools_view(self, request):
        return qa_tools_view(self, request)

    def manual_message_view(self, request):
        return manual_message_view(self, request)

    def my_test_user_view(self, request):
        return my_test_user_view(self, request)

    def reset_my_onboarding_view(self, request):
        return reset_my_onboarding_view(self, request)

    def onboarding_debug_view(self, request):
        return onboarding_debug_view(self, request)

    def healthcheck_view(self, request):
        return healthcheck_view(self, request)

    def admin_notifications_view(self, request):
        return admin_notifications_view(self, request)

    def _is_developer_mode(self, request) -> bool:
        return settings.ENABLE_ADMIN_DEVELOPER_MODE and getattr(request.user, "is_superuser", False)

    def _custom_item(self, *, name: str, object_name: str, url: str) -> dict:
        return {
            "name": name,
            "object_name": object_name,
            "admin_url": url,
            "add_url": None,
            "view_only": True,
            "perms": {"view": True},
        }

    def _build_custom_item(self, request, model_key: str, label: str) -> dict | None:
        if model_key == "dashboard":
            return self._custom_item(name=label, object_name="Dashboard", url=reverse("admin:index"))

        if model_key == "custom:bot_errors":
            if not can_admin_action(request.user, "health_view"):
                return None
            return self._custom_item(
                name=label,
                object_name="BotErrors",
                url=f"{reverse('admin:bot_events_botevent_changelist')}?success__exact=0",
            )

        if model_key == "custom:qa_tools":
            if not can_use_test_tools(request.user):
                return None
            return self._custom_item(name=label, object_name="QaTools", url=reverse("admin:qa_tools"))

        if model_key == "custom:manual_message":
            if not can_admin_action(request.user, "message_send"):
                return None
            return self._custom_item(name=label, object_name="ManualMessage", url=reverse("admin:manual_message"))

        if model_key == "custom:my_test_user":
            if not can_use_test_tools(request.user):
                return None
            return self._custom_item(name=label, object_name="MyTestUser", url=reverse("admin:my_test_user"))

        if model_key == "custom:reset_my_onboarding":
            if not can_use_test_tools(request.user):
                return None
            return self._custom_item(
                name=label,
                object_name="ResetMyOnboarding",
                url=reverse("admin:reset_my_onboarding"),
            )

        if model_key == "custom:onboarding_debug":
            if not can_use_test_tools(request.user):
                return None
            return self._custom_item(
                name=label,
                object_name="OnboardingDebug",
                url=reverse("admin:onboarding_debug"),
            )

        if model_key == "custom:healthcheck":
            if not can_admin_action(request.user, "health_view"):
                return None
            return self._custom_item(name=label, object_name="Healthcheck", url=reverse("admin:system_health"))

        if model_key == "custom:admin_notifications":
            if not can_admin_action(request.user, "notification_view"):
                return None
            return self._custom_item(
                name=label,
                object_name="AdminNotifications",
                url=reverse("admin:admin_notifications"),
            )

        return None

    def _build_sections(self, request, *, menu_structure, available_models: dict[str, dict]) -> list[dict]:
        sections = []
        for section in menu_structure:
            models = []
            for model_key, model_label in section.items:
                if model_key == "dashboard" or model_key.startswith("custom:"):
                    item = self._build_custom_item(request, model_key, model_label)
                    if item:
                        models.append(item)
                    continue

                model = available_models.get(model_key)
                if not model:
                    continue
                model["name"] = model_label
                models.append(model)

            if models:
                sections.append(
                    {
                        "name": section.label,
                        "app_label": section.key,
                        "app_url": models[0].get("admin_url") or reverse("admin:index"),
                        "has_module_perms": True,
                        "models": models,
                    }
                )
        return sections

    def get_app_list(self, request, app_label=None):
        if app_label is not None:
            return super().get_app_list(request, app_label)

        original_apps = super().get_app_list(request, app_label)
        available_models = {}
        for app in original_apps:
            for model in app.get("models", []):
                model_copy = deepcopy(model)
                model_copy["app_label"] = app["app_label"]
                model_key = f"{app['app_label']}.{model_copy.get('object_name', model_copy['name'])}"
                available_models[model_key] = model_copy

        sections = self._build_sections(request, menu_structure=self.menu_structure, available_models=available_models)
        if self._is_developer_mode(request):
            sections.extend(
                self._build_sections(
                    request,
                    menu_structure=self.technical_menu_structure,
                    available_models=available_models,
                )
            )
        return sections


admin_site = CashFlowAdminSite(name="admin")


class StaffUserAdmin(DjangoUserAdmin):
    list_display = ("username", "email", "role_display", "active_display", "last_login", "date_joined")
    list_per_page = 25
    list_max_show_all = 100
    empty_value_display = "—"
    view_on_site = False
    ordering = ("username",)

    @admin.display(description="Роль", ordering="is_superuser")
    def role_display(self, obj):
        if obj.is_superuser:
            return "Суперадміністратор"
        if obj.is_staff:
            return "Адміністратор"
        return "Без доступу"

    @admin.display(description="Стан", ordering="is_active")
    def active_display(self, obj):
        return boolean_badge(obj.is_active, true_label="Активний", false_label="Вимкнений")

    def _operational_context(self, *, is_changelist: bool) -> dict:
        metadata = ADMIN_PAGE_METADATA["auth.User"]
        context = {
            "operational_section": metadata["section"],
            "operational_description": metadata["description"],
            "operational_is_changelist": is_changelist,
        }
        if is_changelist:
            context["title"] = metadata["title"]
        return context

    def changelist_view(self, request, extra_context=None):
        context = self._operational_context(is_changelist=True)
        context.update(extra_context or {})
        return super().changelist_view(request, extra_context=context)

    def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
        context = self._operational_context(is_changelist=False)
        context.update(extra_context or {})
        return super().changeform_view(request, object_id, form_url, extra_context=context)


User = get_user_model()
if User not in admin_site._registry:
    admin_site.register(User, StaffUserAdmin)
