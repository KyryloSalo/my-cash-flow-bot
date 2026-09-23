from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AdminMenuSection:
    key: str
    label: str
    items: tuple[tuple[str, str], ...]


SECTION_WORKSPACE = "Робочий стіл"
SECTION_USERS = "Користувачі"
SECTION_MONETIZATION = "Монетизація"
SECTION_COMMUNICATIONS = "Комунікації"
SECTION_SYSTEM = "Система"
SECTION_TECHNICAL = "Технічне"


PRIMARY_MENU_SECTIONS = (
    AdminMenuSection("workspace", SECTION_WORKSPACE, (("dashboard", "Щоденний контроль"),)),
    AdminMenuSection(
        "users",
        SECTION_USERS,
        (
            ("users.TelegramUser", "Усі користувачі"),
            ("users.Tag", "Теги користувачів"),
            ("miniapp.PartnerLink", "Партнерські посилання"),
        ),
    ),
    AdminMenuSection(
        "monetization",
        SECTION_MONETIZATION,
        (
            ("subscriptions.Subscription", "Підписки користувачів"),
            ("subscriptions.Payment", "Оплати Vydno"),
            ("subscriptions.BillingProfile", "Картки та автосписання"),
            ("subscriptions.Plan", "Тарифи"),
            ("subscriptions.PromoOffer", "Промокоди та акції"),
            ("subscriptions.TrialRecoveryCampaign", "Recovery trial-користувачів"),
        ),
    ),
    AdminMenuSection(
        "communications",
        SECTION_COMMUNICATIONS,
        (
            ("custom:manual_message", "Написати користувачу"),
            ("broadcasts.Broadcast", "Розсилки"),
            ("polls.PollCampaign", "Опитування"),
            ("support.SupportCase", "Звернення користувачів"),
            ("feedback.FeedbackItem", "Відгуки користувачів"),
            ("users.PushTopic", "Теми push-сповіщень"),
            ("broadcasts.AdminMessageLog", "Історія повідомлень"),
        ),
    ),
    AdminMenuSection(
        "system",
        SECTION_SYSTEM,
        (
            ("custom:healthcheck", "Стан системи"),
            ("custom:bot_errors", "Помилки бота"),
            ("bot_settings.BotSetting", "Налаштування бота"),
            ("custom:admin_notifications", "Сповіщення адміністраторів"),
            ("bot_events.BotEvent", "Події бота"),
            ("audit_log.AdminAuditLog", "Журнал дій"),
            ("auth.User", "Адміністратори"),
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
