from __future__ import annotations

import os
from datetime import timedelta
from decimal import Decimal

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.db import connection
from django.utils import timezone

from accounts.models import Account
from common.test_helpers import ensure_telegram_user_table
from subscriptions.models import BillingProfile, Subscription
from transactions.models import Debt, Transaction
from users.models import TelegramUser, UserAdminState


DEV_TG_USER_ID = int(os.environ.get("MINIAPP_DEV_TG_USER_ID", "1001") or "1001")


def _ensure_table(name: str, ddl: str) -> None:
    if name in connection.introspection.table_names():
        return
    with connection.cursor() as cursor:
        cursor.execute(ddl)


def _ensure_columns(table_name: str, columns: dict[str, str]) -> None:
    existing_columns = {
        row.name
        for row in connection.introspection.get_table_description(connection.cursor(), table_name)
    }
    with connection.cursor() as cursor:
        for column_name, ddl in columns.items():
            if column_name not in existing_columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {ddl}")


def ensure_preview_support_tables() -> None:
    if connection.vendor != "sqlite":
        raise RuntimeError("local_preview_seed.py is intended for sqlite preview databases only.")

    _ensure_table(
        "django_session",
        """
        CREATE TABLE django_session (
          session_key VARCHAR(40) PRIMARY KEY,
          session_data TEXT NOT NULL,
          expire_date DATETIME NOT NULL
        )
        """,
    )
    with connection.cursor() as cursor:
        cursor.execute("CREATE INDEX IF NOT EXISTS django_session_expire_date_idx ON django_session (expire_date)")

    _ensure_table(
        "user_admin_states",
        """
        CREATE TABLE user_admin_states (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          telegram_user_id BIGINT NOT NULL UNIQUE,
          status VARCHAR(32) NOT NULL DEFAULT 'active',
          subscription_status VARCHAR(32) NOT NULL DEFAULT 'none',
          access_scope VARCHAR(32) NOT NULL DEFAULT 'paywall',
          access_source VARCHAR(128) NOT NULL DEFAULT '',
          source VARCHAR(128) NOT NULL DEFAULT '',
          referral_code VARCHAR(128) NOT NULL DEFAULT '',
          pending_start_payload TEXT NOT NULL DEFAULT '',
          timezone VARCHAR(64) NOT NULL DEFAULT '',
          last_action_at DATETIME NULL,
          is_blocked BOOLEAN NOT NULL DEFAULT 0,
          can_receive_messages BOOLEAN NOT NULL DEFAULT 1,
          blocked_bot BOOLEAN NOT NULL DEFAULT 0,
          admin_comment TEXT NOT NULL DEFAULT '',
          is_test_user BOOLEAN NOT NULL DEFAULT 1,
          test_user_notes TEXT NOT NULL DEFAULT '',
          marked_as_test_by_id INTEGER NULL,
          marked_as_test_at DATETIME NULL,
          current_fsm_state VARCHAR(255) NOT NULL DEFAULT '',
          onboarding_payload TEXT NOT NULL DEFAULT '{}',
          last_user_input TEXT NOT NULL DEFAULT '',
          last_bot_response TEXT NOT NULL DEFAULT '',
          last_parse_error TEXT NOT NULL DEFAULT '',
          last_onboarding_event_at DATETIME NULL,
          pending_admin_reset_mode VARCHAR(32) NOT NULL DEFAULT '',
          pending_admin_reset_requested_at DATETIME NULL,
          last_admin_reset_at DATETIME NULL,
          created_at DATETIME NOT NULL,
          updated_at DATETIME NOT NULL
        )
        """,
    )

    _ensure_table(
        "subscriptions",
        """
        CREATE TABLE subscriptions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          telegram_user_id BIGINT NOT NULL,
          plan VARCHAR(64) NOT NULL DEFAULT 'solo',
          plan_ref_id INTEGER NULL,
          promo_offer_id INTEGER NULL,
          status VARCHAR(32) NOT NULL DEFAULT 'trial',
          provider VARCHAR(64) NOT NULL DEFAULT '',
          amount NUMERIC(12,2) NULL,
          currency VARCHAR(16) NOT NULL DEFAULT '',
          started_at DATETIME NULL,
          expires_at DATETIME NULL,
          next_charge_at DATETIME NULL,
          grace_expires_at DATETIME NULL,
          trial_days INTEGER NOT NULL DEFAULT 0,
          payment_id VARCHAR(255) NOT NULL DEFAULT '',
          auto_renew BOOLEAN NOT NULL DEFAULT 0,
          source VARCHAR(32) NOT NULL DEFAULT 'system',
          comment TEXT NOT NULL DEFAULT '',
          created_by_id INTEGER NULL,
          updated_by_id INTEGER NULL,
          created_at DATETIME NOT NULL,
          updated_at DATETIME NOT NULL
        )
        """,
    )

    _ensure_table(
        "billing_profiles",
        """
        CREATE TABLE billing_profiles (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          telegram_user_id BIGINT NOT NULL UNIQUE,
          provider VARCHAR(64) NOT NULL DEFAULT 'monobank',
          wallet_id VARCHAR(255) NOT NULL UNIQUE,
          card_token VARCHAR(512) NOT NULL DEFAULT '',
          masked_pan VARCHAR(64) NOT NULL DEFAULT '',
          status VARCHAR(32) NOT NULL DEFAULT 'active',
          auto_renew_enabled BOOLEAN NOT NULL DEFAULT 1,
          last_charge_status VARCHAR(64) NOT NULL DEFAULT '',
          last_failure_reason TEXT NOT NULL DEFAULT '',
          last_action_url VARCHAR(1024) NOT NULL DEFAULT '',
          last_bound_at DATETIME NULL,
          last_charge_at DATETIME NULL,
          created_at DATETIME NOT NULL,
          updated_at DATETIME NOT NULL
        )
        """,
    )

    existing_columns = {row.name for row in connection.introspection.get_table_description(connection.cursor(), "accounts")}
    statements: list[str] = []
    if "goal_name" not in existing_columns:
        statements.append("ALTER TABLE accounts ADD COLUMN goal_name TEXT NULL")
    if "goal_amount" not in existing_columns:
        statements.append("ALTER TABLE accounts ADD COLUMN goal_amount NUMERIC(18,2) NULL")
    if "goal_date" not in existing_columns:
        statements.append("ALTER TABLE accounts ADD COLUMN goal_date DATE NULL")
    if statements:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)


def ensure_preview_runtime_tables() -> None:
    _ensure_table(
        "accounts",
        """
        CREATE TABLE accounts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_user_id BIGINT NOT NULL,
          family_id BIGINT NULL,
          label TEXT NOT NULL,
          currency TEXT NOT NULL,
          account_type TEXT NOT NULL DEFAULT 'other',
          starting_balance NUMERIC(18,2) NOT NULL DEFAULT 0,
          balance NUMERIC(18,2) NOT NULL DEFAULT 0,
          credit_limit NUMERIC(18,2) NULL,
          monthly_interest_rate NUMERIC(18,4) NULL,
          non_negative_account_type TEXT NOT NULL DEFAULT 'main',
          is_active BOOLEAN NOT NULL DEFAULT 1,
          created_at DATETIME NULL,
          updated_at DATETIME NULL,
          goal_name TEXT NULL,
          goal_amount NUMERIC(18,2) NULL,
          goal_date DATE NULL
        )
        """,
    )
    _ensure_columns(
        "accounts",
        {
            "family_id": "family_id BIGINT NULL",
            "credit_limit": "credit_limit NUMERIC(18,2) NULL",
            "monthly_interest_rate": "monthly_interest_rate NUMERIC(18,4) NULL",
            "non_negative_account_type": "non_negative_account_type TEXT NOT NULL DEFAULT 'main'",
        },
    )
    _ensure_table(
        "transactions",
        """
        CREATE TABLE transactions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_user_id BIGINT NOT NULL,
          date DATE NOT NULL,
          type TEXT NOT NULL,
          amount NUMERIC(18,2) NOT NULL,
          currency TEXT NOT NULL,
          to_amount NUMERIC(18,2) NULL,
          to_currency TEXT NULL,
          fx_rate NUMERIC(18,6) NULL,
          fx_rate_text TEXT NULL,
          fx_rate_source TEXT NULL,
          comment TEXT NULL,
          source TEXT NOT NULL,
          account_id BIGINT NULL,
          category_id BIGINT NULL,
          from_account_id BIGINT NULL,
          to_account_id BIGINT NULL,
          flow_kind TEXT NOT NULL DEFAULT 'normal',
          transfer_subtype TEXT NULL,
          counterparty TEXT NULL,
          debt_action TEXT NULL,
          debt_id BIGINT NULL,
          debt_payment_id BIGINT NULL,
          original_amount NUMERIC(18,2) NULL,
          original_currency TEXT NULL,
          exchange_rate NUMERIC(18,6) NULL,
          category_name_snapshot TEXT NULL,
          created_by_user_id BIGINT NULL,
          updated_by_user_id BIGINT NULL,
          is_deleted BOOLEAN NOT NULL DEFAULT 0,
          deleted_at DATETIME NULL,
          deleted_by_user_id BIGINT NULL,
          created_at DATETIME NULL
        )
        """,
    )
    _ensure_columns(
        "transactions",
        {
            "family_id": "family_id BIGINT NULL",
            "transfer_subtype": "transfer_subtype TEXT NULL",
            "fx_rate_source": "fx_rate_source TEXT NULL",
            "created_by_user_id": "created_by_user_id BIGINT NULL",
            "updated_by_user_id": "updated_by_user_id BIGINT NULL",
            "is_deleted": "is_deleted BOOLEAN NOT NULL DEFAULT 0",
            "deleted_at": "deleted_at DATETIME NULL",
            "deleted_by_user_id": "deleted_by_user_id BIGINT NULL",
        },
    )
    _ensure_table(
        "miniapp_write_receipts",
        """
        CREATE TABLE miniapp_write_receipts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_user_id BIGINT NOT NULL,
          operation TEXT NOT NULL,
          idempotency_key TEXT NOT NULL,
          draft_id TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          result TEXT NOT NULL DEFAULT '{}',
          created_at DATETIME NOT NULL,
          updated_at DATETIME NOT NULL,
          UNIQUE (tg_user_id, operation, idempotency_key)
        )
        """,
    )
    _ensure_table(
        "categories",
        """
        CREATE TABLE categories (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_user_id BIGINT NULL,
          user_id BIGINT NULL,
          family_id BIGINT NULL,
          created_by_user_id BIGINT NULL,
          template_id BIGINT NULL,
          kind TEXT NULL,
          type TEXT NULL,
          name TEXT NOT NULL,
          slug TEXT NULL,
          aliases TEXT NOT NULL DEFAULT '[]',
          source TEXT NOT NULL DEFAULT 'legacy',
          is_system BOOLEAN NOT NULL DEFAULT 0,
          is_active BOOLEAN NOT NULL DEFAULT 1,
          sort_order INTEGER NOT NULL DEFAULT 0,
          created_at DATETIME NULL,
          updated_at DATETIME NULL,
          deleted_at DATETIME NULL
        )
        """,
    )
    _ensure_table(
        "debts",
        """
        CREATE TABLE debts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_user_id BIGINT NOT NULL,
          family_id BIGINT NULL,
          counterparty_name TEXT NOT NULL,
          direction TEXT NOT NULL,
          initial_amount NUMERIC(18,2) NOT NULL,
          paid_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
          remaining_amount NUMERIC(18,2) NOT NULL,
          currency TEXT NOT NULL,
          account_id BIGINT NULL,
          status TEXT NOT NULL,
          due_date DATE NULL,
          comment TEXT NULL,
          created_at DATETIME NOT NULL,
          updated_at DATETIME NOT NULL,
          closed_at DATETIME NULL
        )
        """,
    )
    _ensure_columns(
        "debts",
        {
            "family_id": "family_id BIGINT NULL",
        },
    )
    _ensure_table(
        "debt_payments",
        """
        CREATE TABLE debt_payments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_user_id BIGINT NOT NULL,
          family_id BIGINT NULL,
          debt_id BIGINT NOT NULL,
          amount NUMERIC(18,2) NOT NULL,
          currency TEXT NOT NULL,
          account_id BIGINT NULL,
          payment_date DATE NOT NULL,
          comment TEXT NULL,
          created_at DATETIME NOT NULL
        )
        """,
    )
    _ensure_columns(
        "debt_payments",
        {
            "family_id": "family_id BIGINT NULL",
        },
    )


def clear_preview_runtime_rows() -> None:
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM debt_payments WHERE tg_user_id = %s", [DEV_TG_USER_ID])
        cursor.execute("DELETE FROM transactions WHERE tg_user_id = %s", [DEV_TG_USER_ID])
        cursor.execute("DELETE FROM debts WHERE tg_user_id = %s", [DEV_TG_USER_ID])
        cursor.execute(
            "DELETE FROM categories WHERE tg_user_id = %s AND is_system = 0",
            [DEV_TG_USER_ID],
        )
        if "account_admin_states" in connection.introspection.table_names():
            cursor.execute(
                """
                DELETE FROM account_admin_states
                WHERE account_id IN (
                  SELECT id FROM accounts WHERE tg_user_id = %s
                )
                """,
                [DEV_TG_USER_ID],
            )
        cursor.execute("DELETE FROM accounts WHERE tg_user_id = %s", [DEV_TG_USER_ID])


def seed_preview_data() -> None:
    now = timezone.now()
    today = timezone.localdate()

    user, _ = TelegramUser.objects.update_or_create(
        tg_user_id=DEV_TG_USER_ID,
        defaults={
            "first_name": "Ihor",
            "last_name": "",
            "username": "miniapp_preview",
            "lang": "uk",
            "base_currency": "UAH",
            "start_date": today - timedelta(days=90),
            "onboarding_completed": True,
            "onboarding_version": 2,
            "created_at": now,
            "last_seen_at": now,
        },
    )

    UserAdminState.objects.update_or_create(
        telegram_user=user,
        defaults={
            "status": UserAdminState.Status.ACTIVE,
            "subscription_status": UserAdminState.SubscriptionStatus.TRIAL,
            "access_scope": UserAdminState.AccessScope.PERSONAL_FULL,
            "access_source": "local_preview",
            "source": "local_preview",
            "timezone": "Europe/Istanbul",
            "is_blocked": False,
            "can_receive_messages": True,
            "blocked_bot": False,
            "is_test_user": True,
            "onboarding_payload": {},
            "created_at": now,
            "updated_at": now,
        },
    )

    Subscription.objects.update_or_create(
        user=user,
        plan="solo",
        defaults={
            "status": Subscription.Status.TRIAL,
            "provider": "monobank",
            "amount": Decimal("499.00"),
            "currency": "UAH",
            "started_at": now - timedelta(days=3),
            "expires_at": now + timedelta(days=27),
            "next_charge_at": now + timedelta(days=27),
            "grace_expires_at": None,
            "trial_days": 30,
            "payment_id": "preview-trial",
            "auto_renew": True,
            "source": Subscription.Source.SYSTEM,
            "comment": "local miniapp preview",
        },
    )

    BillingProfile.objects.update_or_create(
        user=user,
        defaults={
            "provider": "monobank",
            "wallet_id": f"preview-wallet-{DEV_TG_USER_ID}",
            "card_token": "preview-token",
            "masked_pan": "444455******1111",
            "status": BillingProfile.Status.ACTIVE,
            "auto_renew_enabled": True,
            "last_charge_status": "ok",
            "last_failure_reason": "",
            "last_action_url": "",
            "last_bound_at": now - timedelta(days=3),
            "last_charge_at": now - timedelta(days=3),
        },
    )

    clear_preview_runtime_rows()

    preview_categories = [
        ("expense", "Продукти"),
        ("expense", "Кафе та дозвілля"),
        ("expense", "Транспорт"),
        ("expense", "Підписки"),
        ("income", "Зарплата"),
        ("income", "Фріланс"),
    ]
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO categories (
              tg_user_id, user_id, kind, type, name, aliases, source,
              is_system, is_active, sort_order, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, 1, %s, %s, %s)
            """,
            [
                (
                    DEV_TG_USER_ID,
                    DEV_TG_USER_ID,
                    kind,
                    kind,
                    name,
                    "[]",
                    "local_preview",
                    sort_order,
                    now,
                    now,
                )
                for sort_order, (kind, name) in enumerate(preview_categories, start=1)
            ],
        )

    mono = Account.objects.create(
        tg_user=user,
        label="Mono Black",
        currency="UAH",
        account_type="card",
        starting_balance=Decimal("84320"),
        balance=Decimal("84320"),
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    cash = Account.objects.create(
        tg_user=user,
        label="Готівка",
        currency="UAH",
        account_type="cash",
        starting_balance=Decimal("18500"),
        balance=Decimal("18500"),
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    reserve = Account.objects.create(
        tg_user=user,
        label="Подушка безпеки",
        currency="UAH",
        account_type="savings",
        starting_balance=Decimal("62000"),
        balance=Decimal("62000"),
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    invest = Account.objects.create(
        tg_user=user,
        label="ETF portfolio",
        currency="UAH",
        account_type="investment",
        starting_balance=Decimal("160600"),
        balance=Decimal("160600"),
        is_active=True,
        created_at=now,
        updated_at=now,
    )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE accounts
            SET goal_name = %s, goal_amount = %s, goal_date = %s
            WHERE id = %s
            """,
            ["Подушка безпеки", "100000.00", (today + timedelta(days=180)).isoformat(), reserve.id],
        )

    sample_rows = [
        (today, "income", "52000", "UAH", "text", mono.id, "Зарплата"),
        (today - timedelta(days=1), "expense", "1240", "UAH", "text", mono.id, "Продукти"),
        (today - timedelta(days=2), "expense", "480", "UAH", "text", mono.id, "Таксі"),
        (today - timedelta(days=3), "expense", "149", "UAH", "text", mono.id, "Підписки"),
        (today - timedelta(days=5), "expense", "3200", "UAH", "text", cash.id, "Кафе та дозвілля"),
        (today - timedelta(days=7), "expense", "2800", "UAH", "text", mono.id, "Транспорт"),
        (today - timedelta(days=8), "expense", "6100", "UAH", "text", mono.id, "Продукти"),
        (today - timedelta(days=9), "income", "8700", "UAH", "text", cash.id, "Фриланс"),
    ]
    for tx_date, tx_type, amount, currency, source, account_id, category in sample_rows:
        Transaction.objects.create(
            tg_user=user,
            created_by_user_id=user.tg_user_id,
            date=tx_date,
            type=tx_type,
            amount=Decimal(amount),
            currency=currency,
            source=source,
            account_id=account_id,
            flow_kind="normal",
            category_name_snapshot=category,
            created_at=now,
        )

    Transaction.objects.create(
        tg_user=user,
        created_by_user_id=user.tg_user_id,
        date=today - timedelta(days=4),
        type="transfer",
        amount=Decimal("5000"),
        currency="UAH",
        source="manual",
        from_account=mono,
        to_account=reserve,
        flow_kind="transfer",
        comment="Резерв",
        created_at=now,
    )

    Debt.objects.create(
        tg_user=user,
        counterparty_name="Олег",
        direction="receivable",
        initial_amount=Decimal("12500"),
        paid_amount=Decimal("0"),
        remaining_amount=Decimal("12500"),
        currency="UAH",
        account=mono,
        status="active",
        created_at=now,
        updated_at=now,
    )
    Debt.objects.create(
        tg_user=user,
        counterparty_name="Ірина",
        direction="payable",
        initial_amount=Decimal("4800"),
        paid_amount=Decimal("0"),
        remaining_amount=Decimal("4800"),
        currency="UAH",
        account=mono,
        status="active",
        created_at=now,
        updated_at=now,
    )


def main() -> None:
    ensure_telegram_user_table()
    ensure_preview_runtime_tables()
    ensure_preview_support_tables()
    seed_preview_data()
    print(f"Mini App local preview data prepared for tg_user_id={DEV_TG_USER_ID}.")


if __name__ == "__main__":
    main()
