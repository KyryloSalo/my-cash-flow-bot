from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

from category_services import (  # noqa: E402
    CategoryService,
    CategoryTemplateService,
    DEFAULT_INCOME_CATEGORY_NAMES,
    STALE_DEFAULT_INCOME_CATEGORY_NAMES,
    CategoryValidationError,
    DEFAULT_EXPENSE_FALLBACK_SLUG,
    SYSTEM_INCOME_NAME,
)
from expense_category_catalog import DEFAULT_EXPENSE_CATEGORIES  # noqa: E402


class DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class MemoryConn:
    def __init__(self) -> None:
        self.templates: dict[int, dict] = {}
        self.categories: dict[int, dict] = {}
        self.next_template_id = 1
        self.next_category_id = 1

    def transaction(self) -> DummyTx:
        return DummyTx()

    def add_template(
        self,
        *,
        type: str,
        name: str,
        slug: str | None = None,
        aliases: list[str] | None = None,
        sort_order: int = 0,
        is_system: bool = False,
        is_active: bool = True,
        template_id: int | None = None,
    ) -> int:
        template_id = int(template_id or self.next_template_id)
        self.next_template_id = max(self.next_template_id, template_id + 1)
        self.templates[template_id] = {
            "id": template_id,
            "type": type,
            "name": name,
            "slug": slug,
            "aliases": list(aliases or []),
            "sort_order": int(sort_order),
            "is_system": bool(is_system),
            "is_active": bool(is_active),
        }
        return template_id

    def _add_category_row(
        self,
        *,
        user_id: int,
        type: str,
        name: str,
        slug: str | None = None,
        template_id: int | None = None,
        aliases: list[str] | None = None,
        source: str = "custom",
        is_system: bool = False,
        is_active: bool = True,
        sort_order: int = 0,
        category_id: int | None = None,
    ) -> int:
        category_id = int(category_id or self.next_category_id)
        self.next_category_id = max(self.next_category_id, category_id + 1)
        self.categories[category_id] = {
            "id": category_id,
            "tg_user_id": int(user_id),
            "user_id": int(user_id),
            "family_id": None,
            "template_id": int(template_id) if template_id is not None else None,
            "type": type,
            "name": name,
            "slug": slug,
            "aliases": list(aliases or []),
            "source": source,
            "is_system": bool(is_system),
            "is_active": bool(is_active),
            "sort_order": int(sort_order),
            "deleted_at": None,
        }
        return category_id

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM category_templates" in normalized:
            if "WHERE type='expense'" in normalized:
                rows = [dict(row) for row in self.templates.values() if row["type"] == "expense"]
            elif "WHERE type='income'" in normalized:
                rows = [dict(row) for row in self.templates.values() if row["type"] == "income"]
            elif "WHERE type=$1 AND is_active=true" in normalized:
                rows = [dict(row) for row in self.templates.values() if row["type"] == str(args[0]) and row["is_active"]]
            else:
                rows = []
            rows.sort(key=lambda row: (row["sort_order"], row["name"].casefold()))
            return rows

        if "FROM categories" in normalized:
            user_id = int(args[0])
            type_ = str(args[1])
            rows = [
                dict(row)
                for row in self.categories.values()
                if row["user_id"] == user_id and row["family_id"] is None and row["type"] == type_
            ]
            if "AND is_active=true" in normalized:
                rows = [row for row in rows if row["is_active"]]
            rows.sort(key=lambda row: (row["sort_order"], row["name"].casefold()))
            return rows

        return []

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM category_templates" in normalized and "WHERE id=$1" in normalized:
            row = self.templates.get(int(args[0]))
            return dict(row) if row else None
        if "FROM category_templates" in normalized and "WHERE type=$1 AND slug=$2" in normalized:
            for row in self.templates.values():
                if row["type"] == str(args[0]) and row["slug"] == str(args[1]):
                    return dict(row)
            return None
        if "FROM categories" in normalized and "WHERE id=$1 AND user_id=$2 AND family_id IS NULL" in normalized:
            row = self.categories.get(int(args[0]))
            if row and row["user_id"] == int(args[1]) and row["family_id"] is None:
                return dict(row)
            return None
        if "FROM categories" in normalized and "WHERE user_id=$1 AND family_id IS NULL AND template_id=$2" in normalized:
            for row in self.categories.values():
                if row["user_id"] == int(args[0]) and row["family_id"] is None and row["template_id"] == int(args[1]):
                    return dict(row)
            return None
        if "FROM categories" in normalized and "WHERE user_id=$1" in normalized and "AND slug=$3" in normalized:
            for row in self.categories.values():
                if row["user_id"] == int(args[0]) and row["family_id"] is None and row["type"] == str(args[1]) and row["slug"] == str(args[2]):
                    return dict(row)
            return None
        if normalized.startswith("INSERT INTO categories"):
            category_id = self._add_category_row(
                user_id=int(args[0]),
                type=str(args[1]),
                template_id=(int(args[3]) if args[3] is not None else None),
                name=str(args[4]),
                aliases=list(args[5] or []),
                source=str(args[6]),
                is_system=bool(args[7]),
                sort_order=int(args[8]),
                slug=str(args[9]) if args[9] is not None else None,
            )
            return {"id": category_id}
        return None

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT COALESCE(max(sort_order), 0) + 10" in normalized:
            user_id = int(args[0])
            type_ = str(args[1])
            values = [row["sort_order"] for row in self.categories.values() if row["user_id"] == user_id and row["type"] == type_]
            return (max(values) + 10) if values else 10
        return None

    async def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("UPDATE category_templates SET name=$2, slug=$3"):
            row = self.templates[int(args[0])]
            row.update(
                {
                    "name": str(args[1]),
                    "slug": str(args[2]),
                    "aliases": list(args[3] or []),
                    "sort_order": int(args[4]),
                    "is_system": bool(args[5]),
                    "is_active": True,
                }
            )
            return
        if normalized.startswith("UPDATE category_templates SET name=$2, slug=NULL"):
            row = self.templates[int(args[0])]
            row.update(
                {
                    "name": str(args[1]),
                    "slug": None,
                    "aliases": list(args[2] or []),
                    "sort_order": int(args[3]),
                    "is_system": bool(args[4]),
                    "is_active": True,
                }
            )
            return
        if normalized.startswith("INSERT INTO category_templates"):
            if "VALUES ('income'" in normalized:
                self.add_template(
                    type="income",
                    name=str(args[0]),
                    slug=None,
                    aliases=list(args[1] or []),
                    sort_order=int(args[2]),
                    is_system=bool(args[3]),
                    is_active=True,
                )
            else:
                self.add_template(
                    type="expense",
                    name=str(args[0]),
                    slug=str(args[1]),
                    aliases=list(args[2] or []),
                    sort_order=int(args[3]),
                    is_system=bool(args[4]),
                    is_active=True,
                )
            return
        if "UPDATE category_templates SET is_active=false" in normalized:
            allowed = set(args[0])
            if "WHERE type='income'" in normalized:
                for row in self.templates.values():
                    if row["type"] == "income" and row["name"] not in allowed:
                        row["is_active"] = False
                        row["is_system"] = False
            else:
                for row in self.templates.values():
                    if row["type"] == "expense" and (row["slug"] is None or row["slug"] not in allowed):
                        row["is_active"] = False
                        row["is_system"] = False
            return
        if "ON CONFLICT (type, name) DO UPDATE SET" in normalized:
            existing = next((row for row in self.templates.values() if row["type"] == "income" and row["name"] == str(args[0])), None)
            if existing is None:
                self.add_template(
                    type="income",
                    name=str(args[0]),
                    aliases=list(args[1] or []),
                    sort_order=int(args[2]),
                    is_system=bool(args[3]),
                    is_active=True,
                )
            else:
                existing.update({"sort_order": int(args[2]), "is_system": bool(args[3]), "is_active": True})
            return
        if (
            normalized.startswith("UPDATE categories SET is_active=false")
            and "WHERE user_id=$1" in normalized
            and "type='income'" in normalized
        ):
            user_id = int(args[0])
            stale_names = set(args[1] or [])
            inactive_template_ids = {
                int(row["id"])
                for row in self.templates.values()
                if row["type"] == "income" and not row["is_active"]
            }
            for row in self.categories.values():
                if (
                    row["user_id"] == user_id
                    and row["family_id"] is None
                    and row["type"] == "income"
                    and row["is_active"]
                    and row["source"] in {"system", "template"}
                    and (row["name"] in stale_names or row["template_id"] in inactive_template_ids)
                ):
                    row["is_active"] = False
                    row["deleted_at"] = datetime.now()
            return
        if normalized.startswith("UPDATE categories SET name=$2, slug=$3, template_id=$4"):
            row = self.categories[int(args[0])]
            row.update(
                {
                    "name": str(args[1]),
                    "slug": str(args[2]),
                    "template_id": int(args[3]),
                    "aliases": list(args[4] or []),
                    "source": str(args[5]),
                    "is_system": bool(args[6]),
                    "is_active": True,
                    "sort_order": int(args[7]),
                    "deleted_at": None,
                }
            )
            return
        if normalized.startswith("UPDATE categories SET template_id=COALESCE($2, template_id)"):
            row = self.categories[int(args[0])]
            if args[1] is not None:
                row["template_id"] = int(args[1])
            if args[2] is not None:
                row["slug"] = str(args[2])
            if row["source"] != "custom" or str(args[3]) == "system":
                row["source"] = str(args[3])
            if bool(args[4]):
                row["is_system"] = True
            if not row["aliases"]:
                row["aliases"] = list(args[5] or [])
            if row["sort_order"] == 0 and int(args[6] or 0):
                row["sort_order"] = int(args[6])
            if bool(args[7]):
                row["is_active"] = True
                row["deleted_at"] = None
            return
        if normalized.startswith("UPDATE categories SET name=$2, updated_at=now()"):
            row = self.categories[int(args[0])]
            row["name"] = str(args[1])
            return
        if normalized.startswith("UPDATE categories SET is_active=false, deleted_at=now(), updated_at=now()"):
            row = self.categories[int(args[0])]
            row["is_active"] = False
            row["deleted_at"] = datetime.now()
            return


CategoryConn = MemoryConn


class CategoryServiceTests(unittest.IsolatedAsyncioTestCase):
    def _make_conn(self) -> MemoryConn:
        conn = MemoryConn()
        conn.add_template(type="income", name="Зарплата", sort_order=10)
        conn.add_template(type="income", name=SYSTEM_INCOME_NAME, sort_order=20, is_system=True)
        return conn

    async def test_ensure_default_expense_categories_creates_all_canonical_rows(self) -> None:
        conn = self._make_conn()
        service = CategoryService(conn)

        await service.ensureDefaultExpenseCategories(101)

        categories = await service.getUserCategories(101, "expense")
        self.assertEqual(len(categories), len(DEFAULT_EXPENSE_CATEGORIES))
        self.assertEqual([category.slug for category in categories], [item["slug"] for item in DEFAULT_EXPENSE_CATEGORIES])
        self.assertEqual(categories[0].name, "Інше")

    async def test_ensure_default_expense_categories_is_idempotent(self) -> None:
        conn = self._make_conn()
        service = CategoryService(conn)

        await service.ensureDefaultExpenseCategories(101)
        await service.ensureDefaultExpenseCategories(101)

        categories = await service.getUserCategories(101, "expense")
        self.assertEqual(len(categories), len(DEFAULT_EXPENSE_CATEGORIES))
        self.assertEqual(len({category.slug for category in categories}), len(DEFAULT_EXPENSE_CATEGORIES))

    async def test_get_fallback_expense_category_returns_other(self) -> None:
        conn = self._make_conn()
        service = CategoryService(conn)

        fallback = await service.getFallbackExpenseCategory(101)

        self.assertEqual(fallback.slug, DEFAULT_EXPENSE_FALLBACK_SLUG)
        self.assertEqual(fallback.name, "Інше")

    async def test_ensure_default_categories_keeps_income_templates(self) -> None:
        conn = self._make_conn()
        service = CategoryService(conn)

        await service.ensureDefaultCategories(101)

        income_names = [category.name for category in await service.getUserCategories(101, "income")]
        self.assertEqual(income_names, list(DEFAULT_INCOME_CATEGORY_NAMES))
        self.assertIn(SYSTEM_INCOME_NAME, income_names)

    async def test_sync_default_income_templates_deactivates_legacy_templates(self) -> None:
        conn = self._make_conn()
        legacy_id = conn.add_template(type="income", name="Бонус", sort_order=60, is_active=True)
        mojibake_id = conn.add_template(type="income", name="РџСЂРѕРґР°Р¶", sort_order=70, is_active=True)
        template_service = CategoryTemplateService(conn)

        templates = await template_service.syncDefaultIncomeTemplates()

        self.assertEqual([template.name for template in templates], list(DEFAULT_INCOME_CATEGORY_NAMES))
        self.assertFalse(conn.templates[legacy_id]["is_active"])
        self.assertFalse(conn.templates[mojibake_id]["is_active"])

    async def test_ensure_default_categories_hides_stale_template_income_categories(self) -> None:
        conn = self._make_conn()
        legacy_template_id = conn.add_template(type="income", name="Бонус", sort_order=60, is_active=True)
        legacy_name = "РџСЂРѕРґР°Р¶"
        self.assertIn(legacy_name, STALE_DEFAULT_INCOME_CATEGORY_NAMES)
        conn._add_category_row(
            user_id=101,
            type="income",
            name=legacy_name,
            template_id=legacy_template_id,
            source="template",
            sort_order=60,
        )
        conn._add_category_row(
            user_id=101,
            type="income",
            name="Клієнтський проєкт",
            source="custom",
            sort_order=70,
        )
        service = CategoryService(conn)

        await service.ensureDefaultCategories(101)

        income_names = [category.name for category in await service.getUserCategories(101, "income")]
        self.assertNotIn(legacy_name, income_names)
        self.assertIn("Клієнтський проєкт", income_names)
        for name in DEFAULT_INCOME_CATEGORY_NAMES:
            self.assertIn(name, income_names)

    async def test_fixed_default_expense_category_cannot_be_renamed(self) -> None:
        conn = self._make_conn()
        service = CategoryService(conn)
        await service.ensureDefaultExpenseCategories(101)
        groceries = next(category for category in await service.getUserCategories(101, "expense") if category.slug == "groceries")

        with self.assertRaises(CategoryValidationError):
            await service.renameCategory(101, groceries.id, "Супермаркет")

    async def test_sync_default_expense_templates_deactivates_legacy_templates(self) -> None:
        conn = self._make_conn()
        conn.add_template(type="expense", name="Старе", slug=None, sort_order=999, is_active=True)
        template_service = CategoryTemplateService(conn)

        templates = await template_service.syncDefaultExpenseTemplates()

        self.assertEqual(len(templates), len(DEFAULT_EXPENSE_CATEGORIES))
        legacy = next(row for row in conn.templates.values() if row["name"] == "Старе")
        self.assertFalse(legacy["is_active"])


if __name__ == "__main__":
    unittest.main()
