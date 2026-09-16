from __future__ import annotations

from django.db import connection

from users.models import TelegramUser


def ensure_telegram_user_table() -> None:
    if TelegramUser._meta.db_table in connection.introspection.table_names():
        return
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE users (
              tg_user_id BIGINT PRIMARY KEY,
              first_name TEXT NULL,
              last_name TEXT NULL,
              username TEXT NULL,
              lang TEXT NULL,
              base_currency TEXT NULL,
              start_date DATE NULL,
              onboarding_completed BOOLEAN NOT NULL DEFAULT 0,
              onboarding_version INTEGER NOT NULL DEFAULT 0,
              created_at DATETIME NOT NULL,
              last_seen_at DATETIME NULL
            )
            """
        )


def ensure_runtime_finance_tables() -> None:
    existing = set(connection.introspection.table_names())
    aliases_column_sql = (
        "aliases TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]"
        if connection.vendor == "postgresql"
        else "aliases TEXT NOT NULL DEFAULT '[]'"
    )
    with connection.cursor() as cursor:
        if "families" not in existing:
            cursor.execute(
                """
                CREATE TABLE families (
                  id BIGSERIAL PRIMARY KEY,
                  name TEXT NOT NULL,
                  owner_user_id BIGINT NOT NULL,
                  status TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        if "family_members" not in existing:
            cursor.execute(
                """
                CREATE TABLE family_members (
                  id BIGSERIAL PRIMARY KEY,
                  family_id BIGINT NOT NULL,
                  user_id BIGINT NOT NULL,
                  role TEXT NOT NULL,
                  status TEXT NOT NULL,
                  invited_by_user_id BIGINT NULL,
                  joined_at TIMESTAMPTZ NULL,
                  left_at TIMESTAMPTZ NULL,
                  removed_at TIMESTAMPTZ NULL,
                  created_at TIMESTAMPTZ NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        if "accounts" not in existing:
            cursor.execute(
                """
                CREATE TABLE accounts (
                  id BIGSERIAL PRIMARY KEY,
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
                  goal_name TEXT NULL,
                  goal_amount NUMERIC(18,2) NULL,
                  goal_date DATE NULL,
                  is_active BOOLEAN NOT NULL DEFAULT true,
                  created_at TIMESTAMPTZ NULL,
                  updated_at TIMESTAMPTZ NULL
                )
                """
            )
        if "category_templates" not in existing:
            cursor.execute(
                f"""
                CREATE TABLE category_templates (
                  id BIGSERIAL PRIMARY KEY,
                  type TEXT NOT NULL,
                  name TEXT NOT NULL,
                  slug TEXT NULL,
                  {aliases_column_sql},
                  sort_order INT NOT NULL DEFAULT 0,
                  is_system BOOLEAN NOT NULL DEFAULT false,
                  is_active BOOLEAN NOT NULL DEFAULT true,
                  created_at TIMESTAMPTZ NULL,
                  updated_at TIMESTAMPTZ NULL
                )
                """
            )
        if "categories" not in existing:
            cursor.execute(
                f"""
                CREATE TABLE categories (
                  id BIGSERIAL PRIMARY KEY,
                  tg_user_id BIGINT NULL,
                  user_id BIGINT NULL,
                  family_id BIGINT NULL,
                  created_by_user_id BIGINT NULL,
                  template_id BIGINT NULL,
                  kind TEXT NULL,
                  type TEXT NULL,
                  name TEXT NOT NULL,
                  slug TEXT NULL,
                  {aliases_column_sql},
                  source TEXT NOT NULL DEFAULT 'legacy',
                  is_system BOOLEAN NOT NULL DEFAULT false,
                  is_active BOOLEAN NOT NULL DEFAULT true,
                  sort_order INT NOT NULL DEFAULT 0,
                  created_at TIMESTAMPTZ NULL,
                  updated_at TIMESTAMPTZ NULL,
                  deleted_at TIMESTAMPTZ NULL
                )
                """
            )
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_category_templates_type_name ON category_templates (type, name)")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_category_templates_type_slug ON category_templates (type, slug) WHERE slug IS NOT NULL")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_categories_user_type_slug ON categories (user_id, type, slug) WHERE family_id IS NULL AND slug IS NOT NULL")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_categories_family_type_slug ON categories (family_id, type, slug) WHERE family_id IS NOT NULL AND slug IS NOT NULL")
        if "transactions" not in existing:
            cursor.execute(
                """
                CREATE TABLE transactions (
                  id BIGSERIAL PRIMARY KEY,
                  tg_user_id BIGINT NOT NULL,
                  family_id BIGINT NULL,
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
                  is_deleted BOOLEAN NOT NULL DEFAULT false,
                  deleted_at TIMESTAMPTZ NULL,
                  deleted_by_user_id BIGINT NULL,
                  created_at TIMESTAMPTZ NULL
                )
                """
            )
        if "debts" not in existing:
            cursor.execute(
                """
                CREATE TABLE debts (
                  id BIGSERIAL PRIMARY KEY,
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
                  created_at TIMESTAMPTZ NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL,
                  closed_at TIMESTAMPTZ NULL
                )
                """
            )
        if "debt_payments" not in existing:
            cursor.execute(
                """
                CREATE TABLE debt_payments (
                  id BIGSERIAL PRIMARY KEY,
                  tg_user_id BIGINT NOT NULL,
                  family_id BIGINT NULL,
                  debt_id BIGINT NOT NULL,
                  amount NUMERIC(18,2) NOT NULL,
                  currency TEXT NOT NULL,
                  account_id BIGINT NULL,
                  payment_date DATE NOT NULL,
                  comment TEXT NULL,
                  created_at TIMESTAMPTZ NOT NULL
                )
                """
            )
