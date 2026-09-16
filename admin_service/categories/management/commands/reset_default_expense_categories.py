from __future__ import annotations

from typing import Iterable

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from categories.default_catalog import (
    DEFAULT_EXPENSE_CATEGORIES,
    DEFAULT_EXPENSE_CATEGORY_SLUGS,
    DEFAULT_EXPENSE_FALLBACK_SLUG,
)


def _placeholders(values: Iterable[object]) -> str:
    items = list(values)
    return ", ".join(["%s"] * len(items))


class Command(BaseCommand):
    help = "Reset legacy default expense categories and recreate the canonical 15-category catalog."

    def handle(self, *args, **options):
        with transaction.atomic():
            self._ensure_schema()
            template_ids = self._sync_templates()
            personal_scopes = self._fetch_personal_scopes()
            family_scopes = self._fetch_family_scopes()

            processed = 0
            for user_id in personal_scopes:
                self._reset_scope(user_id=user_id, family_id=None, template_ids=template_ids)
                processed += 1
            for family_id in family_scopes:
                self._reset_scope(user_id=None, family_id=family_id, template_ids=template_ids)
                processed += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"reset_default_expense_categories completed: {len(personal_scopes)} personal scopes, {len(family_scopes)} family scopes, total {processed}."
            )
        )

    def _ensure_schema(self) -> None:
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE category_templates ADD COLUMN IF NOT EXISTS slug TEXT")
            cursor.execute("ALTER TABLE categories ADD COLUMN IF NOT EXISTS slug TEXT")
            cursor.execute("ALTER TABLE categories ADD COLUMN IF NOT EXISTS family_id BIGINT NULL")
            cursor.execute("ALTER TABLE categories ADD COLUMN IF NOT EXISTS created_by_user_id BIGINT NULL")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_category_templates_type_slug ON category_templates (type, slug) WHERE slug IS NOT NULL")
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_categories_user_type_slug ON categories (user_id, type, slug) WHERE family_id IS NULL AND slug IS NOT NULL"
            )
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_categories_family_type_slug ON categories (family_id, type, slug) WHERE family_id IS NOT NULL AND slug IS NOT NULL"
            )

    def _sync_templates(self) -> dict[str, int]:
        slug_to_id: dict[str, int] = {}
        slugs = [str(item["slug"]) for item in DEFAULT_EXPENSE_CATEGORIES]
        with connection.cursor() as cursor:
            for item in DEFAULT_EXPENSE_CATEGORIES:
                slug = str(item["slug"])
                name = str(item["name"])
                aliases = list(item.get("aliases") or [])
                sort_order = int(item["sort_order"])
                is_system = slug == DEFAULT_EXPENSE_FALLBACK_SLUG
                cursor.execute(
                    """
                    SELECT id
                    FROM category_templates
                    WHERE type='expense' AND (slug=%s OR name=%s)
                    ORDER BY CASE WHEN slug=%s THEN 0 ELSE 1 END, id ASC
                    LIMIT 1
                    """,
                    [slug, name, slug],
                )
                row = cursor.fetchone()
                if row:
                    template_id = int(row[0])
                    cursor.execute(
                        """
                        UPDATE category_templates
                        SET name=%s,
                            slug=%s,
                            aliases=%s,
                            sort_order=%s,
                            is_system=%s,
                            is_active=true,
                            updated_at=now()
                        WHERE id=%s
                        """,
                        [name, slug, aliases, sort_order, is_system, template_id],
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO category_templates (
                            type, name, slug, aliases, sort_order, is_system, is_active, created_at, updated_at
                        )
                        VALUES ('expense', %s, %s, %s, %s, %s, true, now(), now())
                        RETURNING id
                        """,
                        [name, slug, aliases, sort_order, is_system],
                    )
                    template_id = int(cursor.fetchone()[0])
                slug_to_id[slug] = template_id

            cursor.execute(
                f"""
                UPDATE category_templates
                SET is_active=false,
                    is_system=false,
                    updated_at=now()
                WHERE type='expense'
                  AND (slug IS NULL OR slug NOT IN ({_placeholders(slugs)}))
                """,
                slugs,
            )
        return slug_to_id

    def _fetch_personal_scopes(self) -> list[int]:
        with connection.cursor() as cursor:
            cursor.execute("SELECT tg_user_id FROM users ORDER BY tg_user_id ASC")
            return [int(row[0]) for row in cursor.fetchall()]

    def _fetch_family_scopes(self) -> list[int]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT family_id
                FROM (
                    SELECT family_id FROM categories WHERE family_id IS NOT NULL
                    UNION
                    SELECT family_id FROM transactions WHERE family_id IS NOT NULL
                ) scoped
                ORDER BY family_id ASC
                """
            )
            return [int(row[0]) for row in cursor.fetchall()]

    def _reset_scope(self, *, user_id: int | None, family_id: int | None, template_ids: dict[str, int]) -> None:
        if (user_id is None) == (family_id is None):
            raise ValueError("exactly one of user_id or family_id must be provided")

        scope_sql = "user_id=%s AND family_id IS NULL" if family_id is None else "family_id=%s"
        scope_value = user_id if family_id is None else family_id
        slugs = list(DEFAULT_EXPENSE_CATEGORY_SLUGS)

        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT id
                FROM categories
                WHERE {scope_sql}
                  AND type='expense'
                  AND (slug IS NULL OR slug NOT IN ({_placeholders(slugs)}))
                """,
                [scope_value, *slugs],
            )
            legacy_ids = [int(row[0]) for row in cursor.fetchall()]

            if legacy_ids:
                cursor.execute(
                    f"""
                    UPDATE transactions
                    SET category_id=NULL,
                        category_name_snapshot=NULL
                    WHERE type='expense'
                      AND category_id IN ({_placeholders(legacy_ids)})
                      AND {"tg_user_id=%s AND family_id IS NULL" if family_id is None else "family_id=%s"}
                    """,
                    [*legacy_ids, scope_value],
                )
                cursor.execute(
                    f"""
                    UPDATE categories
                    SET is_active=false,
                        is_system=false,
                        slug=NULL,
                        deleted_at=now(),
                        updated_at=now()
                    WHERE id IN ({_placeholders(legacy_ids)})
                    """,
                    legacy_ids,
                )

            for item in DEFAULT_EXPENSE_CATEGORIES:
                slug = str(item["slug"])
                name = str(item["name"])
                aliases = list(item.get("aliases") or [])
                sort_order = int(item["sort_order"])
                is_system = slug == DEFAULT_EXPENSE_FALLBACK_SLUG
                template_id = int(template_ids[slug])
                cursor.execute(
                    f"""
                    SELECT id
                    FROM categories
                    WHERE {scope_sql}
                      AND type='expense'
                      AND (slug=%s OR template_id=%s OR name=%s)
                    ORDER BY CASE WHEN slug=%s THEN 0 WHEN template_id=%s THEN 1 ELSE 2 END, id ASC
                    LIMIT 1
                    """,
                    [scope_value, slug, template_id, name, slug, template_id],
                )
                row = cursor.fetchone()
                source = "system" if is_system else "template"
                if row:
                    cursor.execute(
                        """
                        UPDATE categories
                        SET name=%s,
                            slug=%s,
                            template_id=%s,
                            aliases=%s,
                            source=%s,
                            is_system=%s,
                            is_active=true,
                            sort_order=%s,
                            deleted_at=NULL,
                            updated_at=now()
                        WHERE id=%s
                        """,
                        [name, slug, template_id, aliases, source, is_system, sort_order, int(row[0])],
                    )
                    continue

                if family_id is None:
                    cursor.execute(
                        """
                        INSERT INTO categories (
                            tg_user_id, user_id, family_id, created_by_user_id, kind, type,
                            template_id, name, slug, aliases, source, is_system, is_active,
                            sort_order, created_at, updated_at
                        )
                        VALUES (%s, %s, NULL, %s, 'expense', 'expense', %s, %s, %s, %s, %s, %s, true, %s, now(), now())
                        """,
                        [user_id, user_id, user_id, template_id, name, slug, aliases, source, is_system, sort_order],
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO categories (
                            tg_user_id, user_id, family_id, created_by_user_id, kind, type,
                            template_id, name, slug, aliases, source, is_system, is_active,
                            sort_order, created_at, updated_at
                        )
                        VALUES (NULL, NULL, %s, NULL, 'expense', 'expense', %s, %s, %s, %s, %s, %s, true, %s, now(), now())
                        """,
                        [family_id, template_id, name, slug, aliases, source, is_system, sort_order],
                    )
