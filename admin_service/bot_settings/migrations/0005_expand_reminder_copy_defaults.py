from django.db import migrations


def seed_bot_settings(apps, schema_editor):
    BotSetting = apps.get_model("bot_settings", "BotSetting")
    defaults = [
        (
            "saving_reminder_text",
            "Котику, нагадую про сейв.\n\nПлан був такий:\n{amount_text} у \"{target_label}\".\n\nЯкщо вже переказав(ла), запишемо це в боті.\nЯкщо ні, зараз гарний момент зробити переказ.",
            "Automatic reminder text for savings tasks.",
            "string",
        ),
        (
            "saving_after_income_text",
            "Дохід збережено: +{amount_text}.\n\n{percent_text} від цього доходу: {saving_amount_text}.\n\nМожна одразу відкласти частину в накопичення.",
            "Prompt shown after income is saved.",
            "string",
        ),
        (
            "saving_setup_text",
            "Дохід збережено: +{amount_text}.\n\n{percent_text} від цього доходу: {saving_amount_text}.\n\nМожна одразу відкласти частину в накопичення.\n\nЩоб відкладати гроші, створи перше накопичення: банку, сейф, окрему картку або інше місце для відкладених грошей.",
            "Prompt shown after income when no savings account exists.",
            "string",
        ),
        (
            "saving_plan_text",
            "План є.\n\nПереказати:\n{amount_text}\n\nЗвідки:\n{source_label}\n\nКуди:\n{target_label}\n\nТепер зроби реальний переказ у банку. Коли гроші поїхали, натисни \"Я переказав(ла)\".",
            "Text shown after creating a savings plan.",
            "string",
        ),
        (
            "saving_topup_confirm_text",
            "Поповнити накопичення?\n\nЗвідки:\n{source_label}\n\nКуди:\n{target_label}\n\nСума:\n{amount_text}\n\nПеревір: гроші вже реально переказані або відкладені.\nЦе внутрішній переказ, він не потрапить у витрати.",
            "Confirmation text before recording savings topup.",
            "string",
        ),
        (
            "saving_post_income_confirm_text",
            "Відкласти в накопичення?\n\nЗвідки:\n{source_label}\n\nКуди:\n{target_label}\n\nСума:\n{amount_text}\n\nСпочатку зроби реальний переказ у банку або відклади гроші фізично.\nКоли гроші вже переїхали, натисни кнопку нижче.",
            "Confirmation text for post-income savings transfer.",
            "string",
        ),
        (
            "debt_invite_post_create_text",
            "✅ Борг збережено.\n\nХто дав: ти\nХто отримав: {borrower_name}\nСума боргу: {initial_amount}\nПовернуто: {paid_amount}\nЗалишок: {remaining_amount}\nКоментар: {comment}\n\nХочеш створити лінк для {borrower_name}, щоб бот раз на місяць автоматично нагадував про залишок боргу?",
            "Prompt shown after saving a debt before invite creation.",
            "string",
        ),
        (
            "debt_invite_owner_message_text",
            "🔗 Лінк для {borrower_name} готовий.\n\nНадішли боржнику повідомлення нижче.\n\n------\n\nПривіт! Я зафіксував(ла) у боті борг.\n\nХто дав: {owner_name}\nХто отримав: {borrower_name}\nСума боргу: {initial_amount}\nКоментар: {comment}\n\nПідтверди, будь ласка, що це твій Telegram-акаунт для цього боргу.\n\nПісля підтвердження бот буде раз на місяць автоматично нагадувати тобі про залишок.\n\nВажливо: ти не будеш керувати боргом у боті. Повернення коштів відмічає тільки {owner_name}.\n\n{link}",
            "Owner-facing debt invite message.",
            "string",
        ),
        (
            "debt_invite_share_message_text",
            "Привіт! Я зафіксував(ла) у боті борг.\n\nХто дав: {owner_name}\nХто отримав: {borrower_name}\nСума боргу: {initial_amount}\nКоментар: {comment}\n\nПідтверди, будь ласка, що це твій Telegram-акаунт для цього боргу.\n\nПісля підтвердження бот буде раз на місяць автоматично нагадувати тобі про залишок.\n\nВажливо: ти не будеш керувати боргом у боті. Повернення коштів відмічає тільки {owner_name}.\n\n{link}",
            "Shareable debt invite message.",
            "string",
        ),
        (
            "debt_invite_claim_text",
            "Користувач {lender_name} вказав, що дав тобі в борг:\n\nСума: {initial_amount}\nБоржник у записі: {counterparty_name}\nКоментар: {comment}\n\nПісля підтвердження бот буде раз на місяць надсилати тобі автоматичне нагадування про залишок боргу.\n\nВажливо: керує цим боргом тільки {lender_name}. Ти не зможеш редагувати суму, закривати борг або відмічати повернення в боті.\n\nПідтверджуєш, що це твій Telegram-акаунт для цього боргу?",
            "Borrower confirmation text for a debt invite.",
            "string",
        ),
        (
            "debt_monthly_reminder_text",
            "Дружнє нагадування про борг.\n\n{lender_name} дав(ла) тобі в борг: {initial_amount}\nПовернуто за записом {lender_name}: {paid_amount}\nЗалишок: {remaining_amount}\n\nЦе автоматичне щомісячне нагадування. Якщо ти вже повернув(ла) кошти, попроси {lender_name} відмітити це в боті.",
            "Monthly debt reminder text.",
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
        ("bot_settings", "0004_expand_copy_defaults"),
    ]

    operations = [
        migrations.RunPython(seed_bot_settings, noop),
    ]
