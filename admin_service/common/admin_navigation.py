from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AdminMenuSection:
    key: str
    label: str
    items: tuple[tuple[str, str], ...]


SECTION_OVERVIEW = "Огляд"
SECTION_USERS = "Користувачі"
SECTION_VYDNO_SUBSCRIPTIONS = "Підписки Vydno"
SECTION_COMMUNICATIONS = "Комунікації"
SECTION_SUPPORT = "Підтримка"
SECTION_SYSTEM = "Система"
SECTION_MANAGEMENT = "Керування"
SECTION_TECHNICAL = "Технічне"


PRIMARY_MENU_SECTIONS = (
    AdminMenuSection("overview", SECTION_OVERVIEW, (("dashboard", "Огляд"),)),
    AdminMenuSection(
        "users",
        SECTION_USERS,
        (
            ("users.TelegramUser", "Усі користувачі"),
            ("users.Tag", "Теги користувачів"),
        ),
    ),
    AdminMenuSection(
        "vydno_subscriptions",
        SECTION_VYDNO_SUBSCRIPTIONS,
        (
            ("subscriptions.Subscription", "Підписки користувачів"),
            ("subscriptions.Payment", "Оплати за Vydno"),
            ("subscriptions.BillingProfile", "Картки автоплатежів"),
            ("subscriptions.Plan", "Тарифи"),
            ("subscriptions.PromoOffer", "Промокоди й акції"),
        ),
    ),
    AdminMenuSection(
        "communications",
        SECTION_COMMUNICATIONS,
        (
            ("custom:manual_message", "Написати користувачу"),
            ("broadcasts.Broadcast", "Розсилки"),
            ("polls.PollCampaign", "Опитування"),
            ("subscriptions.TrialRecoveryCampaign", "Recovery 90 днів за 1 грн"),
            ("users.PushTopic", "Теми push-сповіщень"),
            ("broadcasts.AdminMessageLog", "Історія повідомлень"),
        ),
    ),
    AdminMenuSection(
        "support",
        SECTION_SUPPORT,
        (
            ("support.SupportCase", "Звернення користувачів"),
            ("feedback.FeedbackItem", "Відгуки користувачів"),
        ),
    ),
    AdminMenuSection(
        "system",
        SECTION_SYSTEM,
        (
            ("custom:healthcheck", "Стан сервісів"),
            ("custom:bot_errors", "Помилки бота"),
            ("bot_events.BotEvent", "Події бота"),
            ("audit_log.AdminAuditLog", "Журнал дій адміністраторів"),
        ),
    ),
    AdminMenuSection(
        "management",
        SECTION_MANAGEMENT,
        (
            ("bot_settings.BotSetting", "Налаштування бота"),
            ("auth.User", "Адміністратори"),
            ("custom:admin_notifications", "Сповіщення для адміністраторів"),
        ),
    ),
)


TECHNICAL_MENU_SECTIONS = (
    AdminMenuSection(
        "technical",
        SECTION_TECHNICAL,
        (
            ("custom:my_test_user", "Мій тестовий користувач"),
            ("custom:reset_my_onboarding", "Скинути мій онбординг"),
            ("custom:onboarding_debug", "Діагностика онбордингу"),
            ("custom:qa_tools", "QA-інструменти"),
            ("broadcasts.Segment", "Сегменти розсилок"),
            ("broadcasts.BroadcastRecipient", "Доставка розсилок"),
            ("categories.CategoryTemplate", "Системні шаблони категорій"),
            ("admin_notifications.AdminNotificationLog", "Доставка сповіщень адміністраторам"),
            ("subscriptions.SubscriptionEvent", "Історія змін підписок"),
            ("support.SupportMessage", "Повідомлення у зверненнях"),
            ("polls.PollRecipient", "Доставка опитувань"),
            ("polls.PollResponse", "Відповіді на опитування"),
            ("subscriptions.PromoOfferClaim", "Активації промокодів"),
            ("subscriptions.TrialRecoveryRecipient", "Отримувачі recovery trial"),
            ("subscriptions.TrialRecoveryDelivery", "Доставки recovery trial"),
        ),
    ),
)


MENU_MODEL_LABELS = {
    model_key: label
    for section in (*PRIMARY_MENU_SECTIONS, *TECHNICAL_MENU_SECTIONS)
    for model_key, label in section.items
    if "." in model_key and not model_key.startswith("custom:")
}
