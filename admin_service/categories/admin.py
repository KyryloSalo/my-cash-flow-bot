from __future__ import annotations

from django import forms
from django.contrib import admin, messages
from django.urls import reverse

from categories.default_catalog import DEFAULT_EXPENSE_CATEGORY_SLUGS
from categories.models import Category, CategoryTemplate
from common.admin import AuditedModelAdmin, HiddenFromMenuAdminMixin, PrivateFinancialDataAdminMixin
from common.admin_actions import bulk_action_confirmation
from common.admin_site import admin_site
from common.admin_ui import boolean_badge, user_identity

Category._meta.verbose_name = "Категорія"
Category._meta.verbose_name_plural = "Категорії"
CategoryTemplate._meta.verbose_name = "Шаблон категорії"
CategoryTemplate._meta.verbose_name_plural = "Шаблони категорій"


class CategoryTemplateAdminForm(forms.ModelForm):
    class Meta:
        model = CategoryTemplate
        fields = ("name", "slug", "type", "aliases", "sort_order", "is_system", "is_active")
        labels = {
            "name": "Назва",
            "slug": "Slug",
            "type": "Тип",
            "aliases": "Аліаси",
            "sort_order": "Порядок",
            "is_system": "Системна",
            "is_active": "Активна",
        }


class CategoryAdminForm(forms.ModelForm):
    class Meta:
        model = Category
        fields = ("user", "template", "name", "slug", "type", "aliases", "is_system", "is_active", "sort_order")
        labels = {
            "user": "Користувач",
            "template": "Системний шаблон",
            "name": "Назва",
            "slug": "Slug",
            "type": "Тип",
            "aliases": "Аліаси",
            "is_system": "Системна категорія",
            "is_active": "Активна",
            "sort_order": "Порядок",
        }


def _is_fixed_expense_template(obj: CategoryTemplate | None) -> bool:
    return bool(obj and obj.type == "expense" and str(obj.slug or "").strip().lower() in DEFAULT_EXPENSE_CATEGORY_SLUGS)


def _is_fixed_expense_category(obj: Category | None) -> bool:
    return bool(obj and obj.category_type == "expense" and str(obj.slug or "").strip().lower() in DEFAULT_EXPENSE_CATEGORY_SLUGS)


@admin.register(CategoryTemplate, site=admin_site)
class CategoryTemplateAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "category_template"
    form = CategoryTemplateAdminForm
    list_display = ("id", "name", "slug", "type", "is_system", "is_active", "sort_order", "updated_at")
    list_filter = ("type", "is_system", "is_active")
    search_fields = ("name", "slug")
    readonly_fields = ("created_at", "updated_at")
    fields = ("name", "slug", "type", "aliases", "sort_order", "is_system", "is_active", "created_at", "updated_at")

    def get_readonly_fields(self, request, obj=None):
        readonly = list(super().get_readonly_fields(request, obj))
        if _is_fixed_expense_template(obj):
            readonly.extend(["name", "slug", "type", "aliases", "sort_order", "is_system", "is_active"])
        return readonly

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Category, site=admin_site)
class CategoryAdmin(PrivateFinancialDataAdminMixin, AuditedModelAdmin):
    audit_object_type = "category"
    form = CategoryAdminForm
    list_display = ("name", "type_display", "user_display", "slug", "system_display", "active_display", "updated_at")
    list_display_links = ("name",)
    list_filter = ("type", "is_system", "is_active", "created_at", "updated_at", ("user__admin_state__is_test_user", admin.BooleanFieldListFilter))
    search_fields = ("name", "slug", "user__username", "tg_user__username", "user__tg_user_id", "tg_user__tg_user_id")
    readonly_fields = ("created_at", "updated_at", "deleted_at", "source")
    fields = (
        "user",
        "template",
        "name",
        "slug",
        "type",
        "aliases",
        "source",
        "is_system",
        "is_active",
        "sort_order",
        "created_at",
        "updated_at",
        "deleted_at",
    )

    @admin.action(description="Деактивувати категорію")
    def deactivate_categories(self, request, queryset):
        if request.POST.get("confirm_action") != "1":
            return bulk_action_confirmation(
                modeladmin=self,
                request=request,
                queryset=queryset,
                title="Деактивувати вибрані категорії?",
                warning=(
                    "Категорії збережуться в історичних операціях, але більше не пропонуватимуться для нових записів. "
                    "Захищені системні категорії витрат не будуть змінені."
                ),
                submit_label="Деактивувати категорії",
                section="Фінанси · Категорії",
                cancel_url=reverse("admin:categories_category_changelist"),
            )
        editable = queryset.exclude(type="expense", slug__in=DEFAULT_EXPENSE_CATEGORY_SLUGS)
        updated = editable.update(is_active=False)
        protected = queryset.count() - updated
        message = f"Деактивовано категорій: {updated}."
        if protected:
            message += f" Захищених системних категорій пропущено: {protected}."
        messages.success(request, message)

    actions = ("deactivate_categories",)

    def get_readonly_fields(self, request, obj=None):
        readonly = list(super().get_readonly_fields(request, obj))
        if _is_fixed_expense_category(obj):
            readonly.extend(["user", "template", "name", "slug", "type", "aliases", "is_system", "is_active", "sort_order"])
        return readonly

    def save_model(self, request, obj, form, change):
        obj.tg_user_id = obj.user_id
        obj.kind = obj.type
        super().save_model(request, obj, form, change)

    def has_delete_permission(self, request, obj=None):
        return False

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("user", "tg_user", "template")

    @admin.display(description="Тип", ordering="type")
    def type_display(self, obj: Category):
        labels = {"expense": "Витрата", "income": "Дохід"}
        return labels.get(obj.category_type, obj.category_type or "—")

    @admin.display(description="Користувач", ordering="user__username")
    def user_display(self, obj: Category):
        return user_identity(obj.user or obj.tg_user)

    @admin.display(description="Системна", ordering="is_system")
    def system_display(self, obj: Category):
        return boolean_badge(obj.is_system)

    @admin.display(description="Стан", ordering="is_active")
    def active_display(self, obj: Category):
        return boolean_badge(obj.is_active, true_label="Активна", false_label="Неактивна")
