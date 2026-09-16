from django.db import migrations


def seed_bot_settings(apps, schema_editor):
    BotSetting = apps.get_model("bot_settings", "BotSetting")
    defaults = [
        (
            "onboarding_welcome_text",
            "Привіт! Це My Cash Flow Bot.\n\nЗараз швидко пройдемо стартове налаштування: мова, базова валюта, стартова дата, рахунки та категорії.\n\nКрок 1/6: Обери мову.",
            "Welcome text and first step of onboarding.",
            "string",
        ),
        (
            "onboarding_restart_notice_text",
            "🔄 Запускаю онбординг повторно для цього акаунта.\n\nПоточні транзакції не чіпаю. За потреби рахунки можна буде відредагувати на кроці підтвердження.",
            "Notice before restarting onboarding.",
            "string",
        ),
        (
            "onboarding_currency_step_text",
            "Крок 2/6: Базова валюта.\n\nУ цій валюті бот показуватиме звіти та підказки за замовчуванням.",
            "Onboarding currency step text.",
            "string",
        ),
        (
            "onboarding_start_date_step_text",
            "Крок 3/6: Стартова дата.\n\nЗ якого дня рахувати звіти? Можеш натиснути кнопку або ввести дату у форматі `dd.mm.yyyy`.",
            "Onboarding start date step text.",
            "string",
        ),
        (
            "onboarding_categories_step_text",
            "Крок 5/6: Категорії\n\nОбери, як налаштувати категорії витрат.\nКатегорії доходів створимо автоматично, а за потреби їх можна буде змінити пізніше.\n\n✅ Стандартні категорії - швидкий старт з готовим набором.\n✏️ Налаштувати вручну - обереш потрібні категорії витрат і за потреби додаси свої.",
            "Onboarding category mode explanation.",
            "string",
        ),
        (
            "onboarding_categories_custom_text",
            "Налаштування категорій\n\nМожеш залишити стандартні категорії витрат, прибрати зайві або додати свої.\nКатегорії доходів на онбордингу створюються автоматично.",
            "Onboarding custom category setup explanation.",
            "string",
        ),
        (
            "support_intro_text",
            "<b>🆘 Допомога</b>\n\nОпишіть проблему одним повідомленням.\n\nАдміністратор побачить ваше звернення і зможе відповісти.",
            "Support flow intro text.",
            "string",
        ),
        (
            "settings_intro_text",
            "<b>⚙️ Налаштування</b>\nОберіть, що хочете змінити:",
            "Settings menu intro text.",
            "string",
        ),
        (
            "reports_intro_text",
            "<b>📊 Звіти</b>\nОберіть період:",
            "Reports menu intro text.",
            "string",
        ),
        (
            "export_intro_text",
            "<b>📤 Експорт Excel</b>\n\nОберіть період, за який потрібно сформувати файл.",
            "Export menu intro text.",
            "string",
        ),
        (
            "categories_intro_text",
            "Категорії допомагають групувати витрати й доходи.\n\nОбери, що хочеш зробити:",
            "Categories menu intro text.",
            "string",
        ),
        (
            "family_disabled_text",
            "<b>👨‍👩‍👧 Сімейний доступ</b>\n\nФункцію тимчасово вимкнено.",
            "Family feature disabled text.",
            "string",
        ),
        (
            "family_empty_text",
            "<b>👨‍👩‍👧 Сімейний доступ</b>\n\nУ вас ще немає сімейного бюджету.\n\nСтворіть родину, щоб вести фінанси разом з партнером або членами сім’ї.",
            "Empty-state text for family feature.",
            "string",
        ),
        (
            "accounts_empty_text",
            "<b>💼 Рахунки</b>\n\nУ вас ще немає жодного рахунку.\n\nЩоб почати облік, додайте перший рахунок.",
            "Empty-state text for accounts list.",
            "string",
        ),
        (
            "savings_empty_text",
            "<b>🐿️ Заощадження та інвестиції</b>\n\nПоки тут порожньо.\n\nСтвори перший рахунок накопичень: накопичення, депозит або інвестрахунок, де ти реально тримаєш відкладені гроші.",
            "Empty-state text for savings overview.",
            "string",
        ),
        (
            "savings_missing_account_text",
            "Рахунок накопичень не знайдено.",
            "Message shown when savings account is missing.",
            "string",
        ),
        (
            "savings_need_first_account_text",
            "Спочатку додай рахунок накопичень.",
            "Message shown when the user needs a savings account first.",
            "string",
        ),
        (
            "hidden_categories_empty_text",
            "Прихованих категорій немає.",
            "Message shown when there are no hidden categories.",
            "string",
        ),
    ]
    for key, value, description, value_type in defaults:
        BotSetting.objects.get_or_create(
            key=key,
            defaults={
                "value": value,
                "description": description,
                "value_type": value_type,
            },
        )


def noop(apps, schema_editor):
    return None


class Migration(migrations.Migration):
    dependencies = [
        ("bot_settings", "0003_expand_defaults"),
    ]

    operations = [
        migrations.RunPython(seed_bot_settings, noop),
    ]
