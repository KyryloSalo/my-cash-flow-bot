from __future__ import annotations

from django.conf import settings
from unfold.admin import ModelAdmin as UnfoldModelAdmin

from .admin_navigation import MENU_MODEL_LABELS
from .audit import create_audit_log, serialize_instance


ADMIN_PAGE_METADATA = {
    "auth.User": {
        "section": "Система",
        "description": "Адміністратори, які мають доступ до Vydno Control. Перевіряйте активність, роль і дату останнього входу перед зміною прав.",
        "ordering": ("username",),
    },
    "users.TelegramUser": {
        "section": "Користувачі",
        "description": "Пошук людей, перевірка доступу, онбордингу, підписки, активності та операторських нотаток.",
        "ordering": ("-last_seen_at", "-created_at"),
    },
    "users.Tag": {
        "section": "Користувачі",
        "description": "Сегменти для роботи з аудиторією, розсилками та внутрішньою CRM-класифікацією.",
        "ordering": ("name",),
    },
    "users.PushTopic": {
        "section": "Комунікації",
        "description": "Теми push-повідомлень і доступність цих тем для користувачів.",
        "ordering": ("-is_active", "name"),
    },
    "miniapp.PartnerLink": {
        "section": "Користувачі",
        "description": "Керовані посилання для каналів трафіку з переходами, реєстраціями та наступними funnel-конверсіями.",
        "ordering": ("-created_at",),
    },
    "accounts.Account": {
        "section": "Приватні фінансові дані",
        "description": "Приватні рахунки й баланси користувачів. Звичайний інтерфейс адміністратора навмисно закритий.",
        "ordering": ("-is_active", "-updated_at"),
    },
    "accounts.AccountAdminState": {
        "section": "Приватні фінансові дані",
        "description": "Службові ознаки приватних рахунків. Звичайний інтерфейс адміністратора навмисно закритий.",
        "ordering": ("-updated_at",),
    },
    "transactions.Transaction": {
        "section": "Приватні фінансові дані",
        "description": "Приватна фінансова історія користувачів. Звичайний інтерфейс адміністратора навмисно закритий.",
        "ordering": ("-date", "-created_at"),
    },
    "transactions.Debt": {
        "section": "Приватні фінансові дані",
        "description": "Приватні борги користувачів. Звичайний інтерфейс адміністратора навмисно закритий.",
        "ordering": ("-updated_at",),
    },
    "transactions.DebtPayment": {
        "section": "Приватні фінансові дані",
        "description": "Приватна історія погашень боргів. Звичайний інтерфейс адміністратора навмисно закритий.",
        "ordering": ("-payment_date", "-created_at"),
    },
    "categories.Category": {
        "section": "Приватні фінансові дані",
        "description": "Приватні категорії користувачів. Звичайний інтерфейс адміністратора навмисно закритий.",
        "ordering": ("type", "name"),
    },
    "categories.CategoryTemplate": {
        "section": "Технічне",
        "description": "Системні шаблони категорій і стабільні slug-ключі runtime-каталогу.",
        "ordering": ("type", "sort_order", "name"),
    },
    "subscriptions.Subscription": {
        "section": "Монетизація",
        "description": "Поточний доступ користувачів: тариф, статус, джерело, строк дії та наступні billing-події.",
        "ordering": ("-updated_at",),
    },
    "subscriptions.Payment": {
        "section": "Монетизація",
        "description": "Оплати й повернення з provider-статусами, сумами та безпечним переглядом технічної відповіді.",
        "ordering": ("-created_at",),
    },
    "subscriptions.BillingProfile": {
        "section": "Монетизація",
        "description": "Стан прив'язаної картки та автоподовження без показу секретного card token.",
        "ordering": ("-updated_at",),
    },
    "subscriptions.Plan": {
        "section": "Монетизація",
        "description": "Тарифні плани, ціни, тривалість і видимість пропозицій.",
        "ordering": ("display_order", "name"),
    },
    "subscriptions.PromoOffer": {
        "section": "Монетизація",
        "description": "Промопропозиції, строки дії, ліміти використання та джерела залучення.",
        "ordering": ("-is_active", "-created_at"),
    },
    "subscriptions.PromoOfferClaim": {
        "section": "Технічне",
        "description": "Службовий журнал активацій промопропозицій користувачами.",
        "ordering": ("-created_at",),
    },
    "subscriptions.SubscriptionEvent": {
        "section": "Технічне",
        "description": "Незмінний журнал подій життєвого циклу підписок.",
        "ordering": ("-created_at",),
    },
    "subscriptions.TrialRecoveryCampaign": {
        "section": "Монетизація",
        "description": "Зрозумілий контроль recovery-кампанії: прогрес за сьогодні, охоплення, відповіді, контакти, причини відмови, доставки та відновлені користувачі.",
        "ordering": ("-created_at",),
    },
    "subscriptions.TrialRecoveryRecipient": {
        "section": "Технічне",
        "description": "Службовий стан кожного користувача у recovery-воронці, включно з розкладом, відповіддю, контактом, opt-out і конверсією.",
        "ordering": ("next_send_at", "id"),
    },
    "subscriptions.TrialRecoveryDelivery": {
        "section": "Технічне",
        "description": "Ідемпотентний журнал кожної спроби recovery-доставки з кроком, статусом, часом і безпечною причиною помилки.",
        "ordering": ("-created_at",),
    },
    "broadcasts.Broadcast": {
        "section": "Комунікації",
        "description": "Керовані масові повідомлення: чернетка, аудиторія, перевірка, відправлення та результат.",
        "ordering": ("-created_at",),
    },
    "broadcasts.AdminMessageLog": {
        "section": "Комунікації",
        "description": "Історія ручних повідомлень користувачам і результатів доставки.",
        "ordering": ("-created_at",),
    },
    "broadcasts.Segment": {
        "section": "Технічне",
        "description": "Службові правила сегментації аудиторії для комунікацій.",
        "ordering": ("name",),
    },
    "broadcasts.BroadcastRecipient": {
        "section": "Технічне",
        "description": "Покомпонентний журнал доставки однієї розсилки кожному отримувачу.",
        "ordering": ("-created_at",),
    },
    "polls.PollCampaign": {
        "section": "Комунікації",
        "description": "Опитування: питання, аудиторія, статус запуску та кількість отриманих відповідей.",
        "ordering": ("-created_at",),
    },
    "polls.PollRecipient": {
        "section": "Технічне",
        "description": "Службовий стан доставки опитування окремому користувачу.",
        "ordering": ("-created_at",),
    },
    "polls.PollResponse": {
        "section": "Технічне",
        "description": "Необроблені відповіді користувачів на опитування.",
        "ordering": ("-created_at",),
    },
    "support.SupportCase": {
        "section": "Комунікації",
        "description": "Звернення користувачів, пріоритет, відповідальний адміністратор і вся історія діалогу.",
        "ordering": ("-updated_at",),
    },
    "support.SupportMessage": {
        "section": "Технічне",
        "description": "Окремі повідомлення всередині звернень підтримки.",
        "ordering": ("-created_at",),
    },
    "feedback.FeedbackItem": {
        "section": "Комунікації",
        "description": "Оцінки й текстові відгуки користувачів із можливістю перетворення на звернення.",
        "ordering": ("-created_at",),
    },
    "bot_events.BotEvent": {
        "section": "Система",
        "description": "Події runtime: сценарій, джерело, результат і діагностика помилок.",
        "ordering": ("-created_at",),
    },
    "audit_log.AdminAuditLog": {
        "section": "Система",
        "description": "Незмінний журнал дій операторів із контекстом об'єкта, результатом та IP-адресою.",
        "ordering": ("-created_at",),
    },
    "admin_notifications.AdminNotificationLog": {
        "section": "Технічне",
        "description": "Журнал системних повідомлень, надісланих адміністраторам у Telegram.",
        "ordering": ("-created_at",),
    },
    "bot_settings.BotSetting": {
        "section": "Система",
        "description": "Операційні параметри, тексти й посилання бота. Ключі та типи захищені від випадкової зміни.",
        "ordering": ("key",),
    },
}

for model_key, title in MENU_MODEL_LABELS.items():
    if model_key in ADMIN_PAGE_METADATA:
        ADMIN_PAGE_METADATA[model_key]["title"] = title


def _title_case_first(value: str) -> str:
    value = str(value or "").strip()
    return f"{value[:1].upper()}{value[1:]}" if value else ""


class HiddenFromMenuAdminMixin:
    hide_from_menu = True

    def get_model_perms(self, request):
        perms = super().get_model_perms(request)
        if settings.ENABLE_ADMIN_DEVELOPER_MODE and getattr(request.user, "is_superuser", False):
            return perms
        if self.hide_from_menu and any(perms.values()):
            return {}
        return perms


class PrivateFinancialDataAdminMixin:
    """Deny the ordinary admin UI access to a user's private finance records."""

    def get_model_perms(self, request):
        return {"add": False, "change": False, "delete": False, "view": False}

    def has_module_permission(self, request):
        return False

    def has_view_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class OperationalModelAdmin(UnfoldModelAdmin):
    """Shared information architecture for every registry in the web backoffice."""

    list_per_page = 25
    list_max_show_all = 100
    empty_value_display = "—"
    view_on_site = False

    def get_operational_metadata(self) -> dict:
        return ADMIN_PAGE_METADATA.get(self.model._meta.label, {})

    def get_ordering(self, request):
        metadata_ordering = self.get_operational_metadata().get("ordering")
        if metadata_ordering:
            return metadata_ordering
        return super().get_ordering(request)

    def _operational_context(self, *, is_changelist: bool) -> dict:
        metadata = self.get_operational_metadata()
        context = {
            "operational_section": metadata.get("section", "Адмінка"),
            "operational_description": metadata.get("description", ""),
            "operational_is_changelist": is_changelist,
        }
        if is_changelist:
            context["title"] = metadata.get("title") or _title_case_first(self.model._meta.verbose_name_plural)
        return context

    def changelist_view(self, request, extra_context=None):
        context = self._operational_context(is_changelist=True)
        context.update(extra_context or {})
        return super().changelist_view(request, extra_context=context)

    def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
        context = self._operational_context(is_changelist=False)
        context.update(extra_context or {})
        return super().changeform_view(request, object_id, form_url, extra_context=context)


class AuditedModelAdmin(OperationalModelAdmin):
    audit_object_type = None

    def _object_type(self) -> str:
        return self.audit_object_type or self.model._meta.label

    def save_model(self, request, obj, form, change):
        before = None
        if change:
            original = self.model.objects.filter(pk=obj.pk).first()
            if original is not None:
                before = serialize_instance(original)
        super().save_model(request, obj, form, change)
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="updated" if change else "created",
            object_type=self._object_type(),
            object_id=obj.pk,
            before=before,
            after=serialize_instance(obj),
        )

    def delete_model(self, request, obj):
        before = serialize_instance(obj)
        object_id = obj.pk
        super().delete_model(request, obj)
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="deleted",
            object_type=self._object_type(),
            object_id=object_id,
            before=before,
            after={},
        )
