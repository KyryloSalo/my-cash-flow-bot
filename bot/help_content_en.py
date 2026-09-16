from __future__ import annotations

from textwrap import dedent


def _text(value: str) -> str:
    return dedent(value).strip()


EN_FAQ_CONTENT = {
    "home_title": "Help",
    "home_intro": _text(
        """
        Here you'll find answers to common questions about the bot, the Mini App, billing, reports, and your data.

        Choose a topic below. If you still need help, tap "Write to support".
        """
    ),
    "support_title": "Support",
    "support_intro": _text(
        """
        Describe what is not working or what question you have.

        To help us reply faster, include:
        - which bot section you used;
        - what you tapped or sent;
        - what should have happened;
        - what happened instead;
        - a screenshot if useful.
        """
    ),
    "support_missing_url_text": "The support link is not configured yet. Please try again later.",
    "topics": (
        {
            "id": "getting_started",
            "title": "Getting Started",
            "questions": (
                {
                    "id": "what_is_vydno",
                    "title": "What is vydno.capital?",
                    "answer": _text(
                        """
                        vydno.capital is a Telegram bot and Mini App for personal finance tracking.

                        You can record expenses, income, transfers, debts, savings, and investments. The service turns those entries into balances, reports, charts, and exports.
                        """
                    ),
                },
                {
                    "id": "how_to_start",
                    "title": "How do I get started?",
                    "answer": _text(
                        """
                        Tap /start and complete the quick setup.

                        Usually you only need to choose a language, set the base currency, choose the start date for reports, and create your first account. After that you can start adding operations.
                        """
                    ),
                },
                {
                    "id": "base_currency",
                    "title": "What is the base currency?",
                    "answer": _text(
                        """
                        The base currency is the main currency used for totals and reports.

                        For example, if your base currency is UAH, balances in USD or EUR will be converted into UAH for the overall picture.
                        """
                    ),
                },
                {
                    "id": "need_everything_now",
                    "title": "Do I need to set up everything at once?",
                    "answer": _text(
                        """
                        No.

                        One account is enough to start. You can add debts, savings, investments, and family access later when you actually need them.
                        """
                    ),
                },
            ),
        },
        {
            "id": "balances_accounts",
            "title": "Balances and Accounts",
            "questions": (
                {
                    "id": "what_is_balances",
                    "title": "What does Balances mean?",
                    "answer": _text(
                        """
                        Balances show how much money you currently have across your accounts.

                        This can include cards, cash, bank accounts, savings accounts, deposits, or investment accounts.
                        """
                    ),
                },
                {
                    "id": "what_is_account",
                    "title": "What is an account?",
                    "answer": _text(
                        """
                        An account is a place where your money lives.

                        Examples: a bank card, cash wallet, USD account, savings jar, deposit, or investment account. Every expense, income, or transfer is attached to an account.
                        """
                    ),
                },
                {
                    "id": "how_add_account",
                    "title": "How do I add an account?",
                    "answer": _text(
                        """
                        Open Settings, then Accounts, then choose Add account.

                        Enter the name, currency, type, and starting balance.
                        """
                    ),
                },
                {
                    "id": "starting_balance",
                    "title": "What is starting balance?",
                    "answer": _text(
                        """
                        It is the amount that is already on the account when you create it.

                        If your card already has 12,500 UAH, create the account with a starting balance of 12,500 UAH so your reports begin from a realistic point.
                        """
                    ),
                },
                {
                    "id": "balance_mismatch",
                    "title": "Why does the bot balance differ from my bank?",
                    "answer": _text(
                        """
                        The most common reasons are missing operations, duplicates, a wrong account, or a transfer recorded as an expense.

                        Cross-currency operations can also break the picture if the exchange rate was entered incorrectly. The bot only counts what was actually recorded in the ledger.
                        """
                    ),
                },
            ),
        },
        {
            "id": "expenses_income",
            "title": "Expenses and Income",
            "questions": (
                {
                    "id": "add_expense",
                    "title": "How do I add an expense?",
                    "answer": _text(
                        """
                        Tap Expense or just send a short message to the bot.

                        Examples:
                        "Coffee 80"
                        "Groceries 1240 card"
                        "Taxi 260"

                        Before saving, the bot shows a confirmation card so you can review the amount, category, account, and date.
                        """
                    ),
                },
                {
                    "id": "add_income",
                    "title": "How do I add income?",
                    "answer": _text(
                        """
                        Tap Income or send a short text message.

                        Examples:
                        "Salary 35000"
                        "Advance 12000"
                        "Client payment 8000"

                        The bot will suggest the account that received the money before saving.
                        """
                    ),
                },
                {
                    "id": "bot_mistake",
                    "title": "What if the bot parsed it incorrectly?",
                    "answer": _text(
                        """
                        Do not confirm the operation right away.

                        On the confirmation step you can change the amount, category, account, date, comment, or operation type. The ledger is updated only after confirmation.
                        """
                    ),
                },
                {
                    "id": "voice_or_screenshot",
                    "title": "Can I add an expense by voice or screenshot?",
                    "answer": _text(
                        """
                        Yes.

                        You can send a short voice message or a payment screenshot. The bot will try to parse the amount and other details, and you will still confirm everything before saving.
                        """
                    ),
                },
                {
                    "id": "edit_saved_transaction",
                    "title": "Can I edit a saved operation?",
                    "answer": _text(
                        """
                        Yes, if the current flow or menu gives you an edit option.

                        The exact set of editable fields depends on the operation type, but the safe path is always: open the operation, review it, then apply the change through the bot flow.
                        """
                    ),
                },
                {
                    "id": "delete_transaction",
                    "title": "Can I delete a saved operation?",
                    "answer": _text(
                        """
                        Yes.

                        Open the operation history, find the record you no longer need, and remove it through the available bot action. If you are not sure which record is correct, contact support before deleting several operations at once.
                        """
                    ),
                },
            ),
        },
        {
            "id": "transfers",
            "title": "Transfers",
            "questions": (
                {
                    "id": "what_is_transfer",
                    "title": "What is a transfer?",
                    "answer": _text(
                        """
                        A transfer is moving money between your own accounts.

                        Examples: card to cash, card to savings, UAH account to USD account, or credit card payoff from another account.
                        """
                    ),
                },
                {
                    "id": "transfer_not_expense",
                    "title": "Why is a transfer not an expense?",
                    "answer": _text(
                        """
                        Because your money did not disappear, it just moved to another place.

                        Transfers change account balances, but they should not distort expense or income reports.
                        """
                    ),
                },
                {
                    "id": "cross_currency_transfer",
                    "title": "How do cross-currency transfers work?",
                    "answer": _text(
                        """
                        For transfers between different currencies, the bot asks for the exchange rate.

                        That rate is required so both account balances and reporting totals stay correct.
                        """
                    ),
                },
            ),
        },
        {
            "id": "categories",
            "title": "Categories",
            "questions": (
                {
                    "id": "why_categories",
                    "title": "Why do I need categories?",
                    "answer": _text(
                        """
                        Categories group expenses and income into understandable buckets.

                        They help with analytics, reports, and spotting where money usually goes.
                        """
                    ),
                },
                {
                    "id": "default_categories",
                    "title": "Are there default categories?",
                    "answer": _text(
                        """
                        Yes.

                        New users receive the standard expense catalog automatically. The bot can also suggest a category when it parses text, voice, or screenshots.
                        """
                    ),
                },
                {
                    "id": "create_category",
                    "title": "Can I create a category?",
                    "answer": _text(
                        """
                        It depends on the current runtime rules for that category type.

                        If the bot offers category creation in your interface, follow that flow. If a category catalog is fixed, use the existing categories and adjust individual operations when needed.
                        """
                    ),
                },
                {
                    "id": "rename_category",
                    "title": "Can I rename a category?",
                    "answer": _text(
                        """
                        If the current category flow allows renaming, open the category settings and choose rename.

                        If the catalog is fixed in your current version, renaming may be unavailable for standard categories.
                        """
                    ),
                },
                {
                    "id": "delete_category",
                    "title": "Can I delete a category?",
                    "answer": _text(
                        """
                        Only when the current category flow allows it.

                        Be careful with deletion: old operations still need a valid category or a fallback so reports remain readable.
                        """
                    ),
                },
            ),
        },
        {
            "id": "debts",
            "title": "Debts",
            "questions": (
                {
                    "id": "what_in_debts",
                    "title": "What is tracked in Debts?",
                    "answer": _text(
                        """
                        The Debts section tracks money you gave to someone or money you owe to someone.

                        It keeps the counterparty, original amount, currency, returned amount, remaining balance, and reminders.
                        """
                    ),
                },
                {
                    "id": "add_debt",
                    "title": "How do I add a debt?",
                    "answer": _text(
                        """
                        Start the debt flow from the debts menu or from text if the parser recognizes it.

                        The bot will ask who is involved, the amount, currency, and optional details before showing a confirmation step.
                        """
                    ),
                },
                {
                    "id": "mark_debt_return",
                    "title": "How do I mark a debt as returned?",
                    "answer": _text(
                        """
                        Open the debt and choose the repayment action.

                        You can record full or partial repayment, and the remaining amount will update automatically.
                        """
                    ),
                },
                {
                    "id": "partial_repayment",
                    "title": "Can I record a partial repayment?",
                    "answer": _text(
                        """
                        Yes.

                        Enter the amount that was returned. The bot recalculates how much is still unpaid and keeps the debt active until the remainder becomes zero.
                        """
                    ),
                },
                {
                    "id": "invite_person_to_debt",
                    "title": "Can I invite the other person to confirm the debt?",
                    "answer": _text(
                        """
                        Yes, when the debt flow offers an invite link.

                        The other person can confirm that the debt is linked to their Telegram account, and the bot can then send reminder messages.
                        """
                    ),
                },
                {
                    "id": "debt_reminders",
                    "title": "Are debt reminders supported?",
                    "answer": _text(
                        """
                        Yes, for supported debt invite flows.

                        After confirmation, the bot can send periodic reminder messages about the remaining balance.
                        """
                    ),
                },
            ),
        },
        {
            "id": "savings",
            "title": "Savings",
            "questions": (
                {
                    "id": "what_is_savings",
                    "title": "What counts as savings?",
                    "answer": _text(
                        """
                        Savings are money set aside for a goal, reserve, or planned purpose.

                        In practice this can be a dedicated savings account, deposit, or another account you use for accumulation instead of daily spending.
                        """
                    ),
                },
                {
                    "id": "create_goal",
                    "title": "Can I create a savings goal?",
                    "answer": _text(
                        """
                        Yes, if the current savings flow is enabled for your account.

                        The bot can show a separate savings surface, the linked account, and progress toward the target amount.
                        """
                    ),
                },
                {
                    "id": "bot_auto_save",
                    "title": "Does the bot save money automatically?",
                    "answer": _text(
                        """
                        Only if the current flow explicitly offers an automatic savings setup.

                        Otherwise, top-ups and withdrawals are still recorded through normal guided actions so balances stay correct.
                        """
                    ),
                },
                {
                    "id": "savings_vs_account",
                    "title": "What is the difference between savings and a regular account?",
                    "answer": _text(
                        """
                        A regular account is where money is stored. Savings is the purpose and tracking layer on top of that.

                        Savings flows help you separate reserved money from daily spending and see progress more clearly.
                        """
                    ),
                },
            ),
        },
        {
            "id": "investments",
            "title": "Investments",
            "questions": (
                {
                    "id": "what_track_in_investments",
                    "title": "What can I track in investments?",
                    "answer": _text(
                        """
                        You can track investment accounts and their current value.

                        This is useful for keeping a single picture of your assets alongside cash, savings, and debts.
                        """
                    ),
                },
                {
                    "id": "add_investment",
                    "title": "How do I add an investment account?",
                    "answer": _text(
                        """
                        Create an account with an investment-type role when that option is available in the account flow.

                        Then use that account for investment-related balances and reporting.
                        """
                    ),
                },
                {
                    "id": "financial_advice",
                    "title": "Does the bot give financial advice?",
                    "answer": _text(
                        """
                        No.

                        The bot helps you record and review data. It does not replace personal investment or financial advice.
                        """
                    ),
                },
                {
                    "id": "investments_vs_savings",
                    "title": "What is the difference between investments and savings?",
                    "answer": _text(
                        """
                        Savings are usually money reserved with lower risk and a clearer short- or mid-term goal.

                        Investments are assets whose value can change and are tracked separately so they do not get mixed with day-to-day spending money.
                        """
                    ),
                },
            ),
        },
        {
            "id": "family",
            "title": "Family Access",
            "questions": (
                {
                    "id": "what_is_family",
                    "title": "What is family access?",
                    "answer": _text(
                        """
                        Family access lets several people work with one shared finance space.

                        This is useful for couples or families who want a common picture of balances, spending, and operations.
                        """
                    ),
                },
                {
                    "id": "family_member_limit",
                    "title": "Is there a member limit?",
                    "answer": _text(
                        """
                        Yes, the flow can enforce a member limit for one family space.

                        If the family is already full, new invite links will not be accepted until a slot becomes available.
                        """
                    ),
                },
                {
                    "id": "invite_family_member",
                    "title": "How do I invite a family member?",
                    "answer": _text(
                        """
                        Open the family access section and create an invite link if the feature is enabled for your account.

                        The other person joins by opening the invite link in Telegram.
                        """
                    ),
                },
                {
                    "id": "who_added_operation",
                    "title": "Can I see who added an operation?",
                    "answer": _text(
                        """
                        In shared spaces, operation ownership depends on the current runtime surface.

                        If the bot shows that detail in the flow or report, you can identify who recorded the operation.
                        """
                    ),
                },
                {
                    "id": "family_balance_visibility",
                    "title": "Can family members see shared balances?",
                    "answer": _text(
                        """
                        Yes, that is the point of shared access.

                        Family members can work with the same shared finance space instead of keeping separate disconnected ledgers.
                        """
                    ),
                },
            ),
        },
        {
            "id": "reports",
            "title": "Reports",
            "questions": (
                {
                    "id": "what_reports_show",
                    "title": "What do reports show?",
                    "answer": _text(
                        """
                        Reports summarize the operations recorded in the bot for the selected period.

                        Depending on the surface, you can see expenses, income, net flow, account balances, debt state, category structure, and other aggregate views.
                        """
                    ),
                },
                {
                    "id": "report_period",
                    "title": "Can I choose the report period?",
                    "answer": _text(
                        """
                        Yes.

                        The bot and Mini App support preset periods and, where available, custom date ranges.
                        """
                    ),
                },
                {
                    "id": "report_inaccurate",
                    "title": "Why does the report look wrong?",
                    "answer": _text(
                        """
                        Reports are only as accurate as the ledger entries behind them.

                        Check missing operations, duplicates, wrong accounts, transfer misclassification, and exchange-rate inputs for cross-currency actions.
                        """
                    ),
                },
            ),
        },
        {
            "id": "miniapp",
            "title": "Mini App",
            "questions": (
                {
                    "id": "what_is_miniapp",
                    "title": "What is the Mini App?",
                    "answer": _text(
                        """
                        The Mini App is the in-Telegram dashboard version of the product.

                        It gives you a broader visual overview with balances, trends, analytics, accounts, debts, and other summary widgets.
                        """
                    ),
                },
                {
                    "id": "what_can_do_in_miniapp",
                    "title": "What can I do in the Mini App?",
                    "answer": _text(
                        """
                        You can review your dashboard, balances, recent activity, analytics, accounts, debts, goals, and investments.

                        The exact sections depend on your current access mode and the data already recorded in the bot.
                        """
                    ),
                },
                {
                    "id": "miniapp_vs_bot",
                    "title": "What is the difference between the Mini App and the bot?",
                    "answer": _text(
                        """
                        The bot is best for fast input and guided actions.

                        The Mini App is best for viewing a richer dashboard and navigating summary information.
                        """
                    ),
                },
                {
                    "id": "miniapp_registration",
                    "title": "Do I need separate registration for the Mini App?",
                    "answer": _text(
                        """
                        No.

                        Open the Mini App from Telegram after you already have a bot account. It uses the same user profile and access state.
                        """
                    ),
                },
            ),
        },
        {
            "id": "export",
            "title": "Data Export",
            "questions": (
                {
                    "id": "what_is_export",
                    "title": "What is export?",
                    "answer": _text(
                        """
                        Export creates a file with your recorded finance data.

                        It is useful when you want to review data outside the bot, keep a backup, or work in spreadsheets.
                        """
                    ),
                },
                {
                    "id": "what_is_in_export",
                    "title": "What is included in export?",
                    "answer": _text(
                        """
                        Export can include normal operations, transfers, adjustments, and debt-related rows, depending on the file format and runtime implementation.

                        The main rule is that export should reflect the ledger rather than only the summary views.
                        """
                    ),
                },
                {
                    "id": "why_export",
                    "title": "Why would I need export?",
                    "answer": _text(
                        """
                        Export is useful for backup, deeper spreadsheet analysis, sharing data with an accountant, or keeping a local archive.
                        """
                    ),
                },
                {
                    "id": "partial_export",
                    "title": "Can I export only part of the data?",
                    "answer": _text(
                        """
                        Yes, usually by selecting a period before export.

                        If the current export flow offers filters, use them before the file is generated.
                        """
                    ),
                },
            ),
        },
        {
            "id": "billing_access",
            "title": "Billing and Access",
            "questions": (
                {
                    "id": "trial_period",
                    "title": "Is there a trial period?",
                    "answer": _text(
                        """
                        Yes, when your current offer includes a trial.

                        The exact number of trial days depends on the active billing flow for your account.
                        """
                    ),
                },
                {
                    "id": "why_bind_card",
                    "title": "Why do I need to bind a card?",
                    "answer": _text(
                        """
                        Card binding is used to activate paid access flows and recurring billing when applicable.

                        The bot can also make a small verification charge for card confirmation, depending on the current billing setup.
                        """
                    ),
                },
                {
                    "id": "when_499_charged",
                    "title": "When is the 499 UAH charge applied?",
                    "answer": _text(
                        """
                        It is charged only after the trial period ends, according to the active billing rules for your account.
                        """
                    ),
                },
                {
                    "id": "cancel_subscription",
                    "title": "Can I disable auto-renew or cancel billing?",
                    "answer": _text(
                        """
                        Yes.

                        Open the billing/settings section and use the available disable or cancel action for your card and subscription state.
                        """
                    ),
                },
                {
                    "id": "trial_returns",
                    "title": "Will the trial or verification payment be returned?",
                    "answer": _text(
                        """
                        The answer depends on the current billing flow.

                        If the product uses a small card-verification charge, that charge is separate from the paid subscription renewal logic. Check the current billing screen or contact support if you need the exact rule for your account.
                        """
                    ),
                },
                {
                    "id": "change_card",
                    "title": "Can I change the linked card?",
                    "answer": _text(
                        """
                        Yes.

                        Use the billing menu to rebind the card when that action is available for your current billing state.
                        """
                    ),
                },
            ),
        },
        {
            "id": "data_privacy",
            "title": "Data and Privacy",
            "questions": (
                {
                    "id": "who_sees_my_data",
                    "title": "Who can see my data?",
                    "answer": _text(
                        """
                        Your data is used to operate your finance account and the related product surfaces.

                        Access is limited to the product logic and authorized support or operations workflows when needed to resolve issues.
                        """
                    ),
                },
                {
                    "id": "manual_analysis",
                    "title": "Does someone manually read all my finances?",
                    "answer": _text(
                        """
                        No.

                        The service is not built for manual browsing of other people's finances. Data is processed so your account, reports, and support workflows can function correctly.
                        """
                    ),
                },
                {
                    "id": "delete_my_data",
                    "title": "Can I delete my data?",
                    "answer": _text(
                        """
                        Yes.

                        Contact support and describe the request. They can help you with the available data deletion flow.
                        """
                    ),
                },
                {
                    "id": "where_read_more_privacy",
                    "title": "Where can I read more about privacy?",
                    "answer": _text(
                        """
                        Read the privacy policy.

                        It explains which data is needed, how it is stored, and why the service uses it.
                        """
                    ),
                },
            ),
        },
        {
            "id": "issues",
            "title": "Bot Issues",
            "questions": (
                {
                    "id": "bot_not_responding",
                    "title": "The bot is not responding. What should I do?",
                    "answer": _text(
                        """
                        Try the following:

                        1. Send /start.
                        2. Reopen Telegram.
                        3. Check your internet connection.
                        4. Repeat the action a few minutes later.

                        If it still fails, tap "Write to support".
                        """
                    ),
                },
                {
                    "id": "wrong_recognition",
                    "title": "The bot recognized the operation incorrectly. What should I do?",
                    "answer": _text(
                        """
                        Do not confirm it immediately.

                        Use the edit step to correct the amount, category, account, or operation type before saving.
                        """
                    ),
                },
                {
                    "id": "duplicate_transaction",
                    "title": "I added the same operation twice by mistake. What should I do?",
                    "answer": _text(
                        """
                        Open the operation history, find the duplicate, and remove it through the available action.

                        If you are unsure which record is correct, contact support first.
                        """
                    ),
                },
                {
                    "id": "what_to_write_to_support",
                    "title": "A button does not work. What should I write to support?",
                    "answer": _text(
                        """
                        Send a short message with:
                        - which button does not work;
                        - which section you were in;
                        - what you tapped;
                        - what should have happened;
                        - what actually happened.

                        A screenshot is also useful.
                        """
                    ),
                },
            ),
        },
    ),
}
