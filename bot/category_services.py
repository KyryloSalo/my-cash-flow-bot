from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

import asyncpg

from expense_category_catalog import (
    DEFAULT_EXPENSE_CATEGORIES,
    DEFAULT_EXPENSE_CATEGORY_BY_SLUG,
    DEFAULT_EXPENSE_CATEGORY_SLUGS,
    DEFAULT_EXPENSE_FALLBACK_SLUG,
)
from finance_scope import FinanceScope, get_current_finance_scope

CategoryType = Literal["expense", "income"]

CATEGORY_NAME_MIN_LENGTH = 2
CATEGORY_NAME_MAX_LENGTH = 40

SYSTEM_EXPENSE_NAME = "Інше"
SYSTEM_INCOME_NAME = "Інший дохід"

FIXED_EXPENSE_CATEGORY_SLUGS = set(DEFAULT_EXPENSE_CATEGORY_SLUGS)

DEFAULT_INCOME_CATEGORIES = [
    {"name": "Зарплата", "sort_order": 10, "is_system": False, "aliases": []},
    {"name": "Фріланс", "sort_order": 20, "is_system": False, "aliases": []},
    {"name": "Подарунок", "sort_order": 30, "is_system": False, "aliases": []},
    {"name": "Кешбек", "sort_order": 40, "is_system": False, "aliases": []},
    {"name": SYSTEM_INCOME_NAME, "sort_order": 50, "is_system": True, "aliases": []},
]
DEFAULT_INCOME_CATEGORY_NAMES = tuple(str(category["name"]) for category in DEFAULT_INCOME_CATEGORIES)
_LEGACY_INCOME_CATEGORY_NAMES = (
    "Зарплата",
    "Фриланс",
    "Бонус",
    "Відсотки",
    "Дивіденди",
    "Кешбек",
    "Повернення боргу",
    "Продаж",
    "Оренда",
    "Подарунки",
    SYSTEM_INCOME_NAME,
)


def _mojibake_utf8_as_cp1251(value: str) -> str | None:
    try:
        repaired = value.encode("utf-8").decode("cp1251")
    except UnicodeError:
        return None
    return repaired if repaired != value else None


STALE_DEFAULT_INCOME_CATEGORY_NAMES = tuple(
    sorted(
        {
            name
            for name in _LEGACY_INCOME_CATEGORY_NAMES
            if name not in DEFAULT_INCOME_CATEGORY_NAMES
        }
        | {
            mojibake
            for name in (*_LEGACY_INCOME_CATEGORY_NAMES, *DEFAULT_INCOME_CATEGORY_NAMES)
            for mojibake in [_mojibake_utf8_as_cp1251(name)]
            if mojibake
        }
    )
)


class CategoryValidationError(ValueError):
    pass


class CategoryConflictError(CategoryValidationError):
    pass


class CategoryUnavailableError(LookupError):
    pass


@dataclass(frozen=True)
class CategoryTemplate:
    id: int
    type: CategoryType
    name: str
    slug: str | None
    aliases: list[str]
    sort_order: int
    is_system: bool
    is_active: bool


@dataclass(frozen=True)
class Category:
    id: int
    user_id: int
    family_id: int | None
    template_id: int | None
    type: CategoryType
    name: str
    slug: str | None
    aliases: list[str]
    source: str
    is_system: bool
    is_active: bool
    sort_order: int
    deleted_at: Any | None


def normalize_category_name(name: str) -> str:
    value = (name or "").replace("\u00A0", " ")
    return re.sub(r"\s+", " ", value).strip()


def validate_category_name(name: str) -> str:
    normalized = normalize_category_name(name)
    if not normalized:
        raise CategoryValidationError("category name is required")
    if len(normalized) < CATEGORY_NAME_MIN_LENGTH or len(normalized) > CATEGORY_NAME_MAX_LENGTH:
        raise CategoryValidationError("category name length is invalid")
    if re.search(r"[\x00-\x1f\x7f]", normalized):
        raise CategoryValidationError("category name contains control characters")
    return normalized


def normalize_category_slug(slug: str | None) -> str | None:
    value = str(slug or "").strip().lower()
    return value or None


def _norm_aliases(aliases: list[str] | None) -> list[str]:
    raw = aliases or []
    out: list[str] = []
    for alias in raw:
        value = normalize_category_name(alias)
        if not value:
            continue
        if value not in out:
            out.append(value)
    return out[:50]


def _same_name(left: str, right: str) -> bool:
    return normalize_category_name(left).casefold() == normalize_category_name(right).casefold()


def is_fixed_default_expense_slug(slug: str | None) -> bool:
    return normalize_category_slug(slug) in FIXED_EXPENSE_CATEGORY_SLUGS


def is_fixed_default_expense_category(category: Category | None) -> bool:
    return bool(category and category.type == "expense" and is_fixed_default_expense_slug(category.slug))


class CategoryTemplateService:
    def __init__(self, conn: asyncpg.Connection):
        self._conn = conn

    def _map_template(self, row: asyncpg.Record) -> CategoryTemplate:
        return CategoryTemplate(
            id=int(row["id"]),
            type=str(row["type"]),
            name=str(row["name"]),
            slug=normalize_category_slug(row["slug"]) if "slug" in row else None,
            aliases=list(row["aliases"] or []),
            sort_order=int(row["sort_order"] or 0),
            is_system=bool(row["is_system"]),
            is_active=bool(row["is_active"]),
        )

    async def getDefaultExpenseTemplates(self) -> list[CategoryTemplate]:
        return await self.getActiveTemplatesByType("expense")

    async def getDefaultIncomeTemplates(self) -> list[CategoryTemplate]:
        return await self.getActiveTemplatesByType("income")

    async def getTemplateById(self, templateId: int) -> CategoryTemplate | None:
        row = await self._conn.fetchrow(
            """
            SELECT id, type, name, slug, aliases, sort_order, is_system, is_active
            FROM category_templates
            WHERE id=$1
            """,
            int(templateId),
        )
        return self._map_template(row) if row else None

    async def getTemplateBySlug(self, type: CategoryType, slug: str) -> CategoryTemplate | None:
        normalized_slug = normalize_category_slug(slug)
        if normalized_slug is None:
            return None
        row = await self._conn.fetchrow(
            """
            SELECT id, type, name, slug, aliases, sort_order, is_system, is_active
            FROM category_templates
            WHERE type=$1 AND slug=$2
            LIMIT 1
            """,
            type,
            normalized_slug,
        )
        return self._map_template(row) if row else None

    async def getActiveTemplatesByType(self, type: CategoryType) -> list[CategoryTemplate]:
        rows = await self._conn.fetch(
            """
            SELECT id, type, name, slug, aliases, sort_order, is_system, is_active
            FROM category_templates
            WHERE type=$1 AND is_active=true
            ORDER BY sort_order ASC, name ASC
            """,
            type,
        )
        return [self._map_template(row) for row in rows]

    async def syncDefaultExpenseTemplates(self) -> list[CategoryTemplate]:
        rows = await self._conn.fetch(
            """
            SELECT id, type, name, slug, aliases, sort_order, is_system, is_active
            FROM category_templates
            WHERE type='expense'
            ORDER BY sort_order ASC, name ASC
            """
        )
        by_slug = {
            normalize_category_slug(row["slug"]): row
            for row in rows
            if normalize_category_slug(row["slug"]) is not None
        }
        by_name = {normalize_category_name(str(row["name"])).casefold(): row for row in rows}

        for category in DEFAULT_EXPENSE_CATEGORIES:
            slug = str(category["slug"])
            name = str(category["name"])
            aliases = _norm_aliases(list(category.get("aliases") or []))
            sort_order = int(category["sort_order"])
            is_system = slug == DEFAULT_EXPENSE_FALLBACK_SLUG
            existing = by_slug.get(slug) or by_name.get(normalize_category_name(name).casefold())
            if existing is not None:
                await self._conn.execute(
                    """
                    UPDATE category_templates
                    SET name=$2,
                        slug=$3,
                        aliases=$4,
                        sort_order=$5,
                        is_system=$6,
                        is_active=true,
                        updated_at=now()
                    WHERE id=$1
                    """,
                    int(existing["id"]),
                    name,
                    slug,
                    aliases,
                    sort_order,
                    is_system,
                )
                continue

            await self._conn.execute(
                """
                INSERT INTO category_templates (
                  type, name, slug, aliases, sort_order, is_system, is_active, created_at, updated_at
                )
                VALUES ('expense', $1, $2, $3, $4, $5, true, now(), now())
                """,
                name,
                slug,
                aliases,
                sort_order,
                is_system,
            )

        await self._conn.execute(
            """
            UPDATE category_templates
            SET is_active=false, is_system=false, updated_at=now()
            WHERE type='expense'
              AND (slug IS NULL OR slug <> ALL($1::text[]))
            """,
            list(DEFAULT_EXPENSE_CATEGORY_SLUGS),
        )
        return await self.getActiveTemplatesByType("expense")

    async def syncDefaultIncomeTemplates(self) -> list[CategoryTemplate]:
        rows = await self._conn.fetch(
            """
            SELECT id, type, name, slug, aliases, sort_order, is_system, is_active
            FROM category_templates
            WHERE type='income'
            ORDER BY sort_order ASC, name ASC
            """
        )
        by_name = {normalize_category_name(str(row["name"])).casefold(): row for row in rows}

        for category in DEFAULT_INCOME_CATEGORIES:
            name = str(category["name"])
            existing = by_name.get(normalize_category_name(name).casefold())
            if existing is not None:
                await self._conn.execute(
                    """
                    UPDATE category_templates
                    SET name=$2,
                        slug=NULL,
                        aliases=$3,
                        sort_order=$4,
                        is_system=$5,
                        is_active=true,
                        updated_at=now()
                    WHERE id=$1
                    """,
                    int(existing["id"]),
                    name,
                    _norm_aliases(list(category.get("aliases") or [])),
                    int(category["sort_order"]),
                    bool(category["is_system"]),
                )
                continue

            await self._conn.execute(
                """
                INSERT INTO category_templates (
                  type, name, slug, aliases, sort_order, is_system, is_active, created_at, updated_at
                )
                VALUES ('income', $1, NULL, $2, $3, $4, true, now(), now())
                """,
                name,
                _norm_aliases(list(category.get("aliases") or [])),
                int(category["sort_order"]),
                bool(category["is_system"]),
            )

        await self._conn.execute(
            """
            UPDATE category_templates
            SET is_active=false, is_system=false, updated_at=now()
            WHERE type='income'
              AND name <> ALL($1::text[])
            """,
            list(DEFAULT_INCOME_CATEGORY_NAMES),
        )
        return await self.getActiveTemplatesByType("income")


class CategoryService:
    def __init__(self, conn: asyncpg.Connection):
        self._conn = conn

    async def _scope(self, userId: int) -> FinanceScope:
        return await get_current_finance_scope(self._conn, userId)

    def _map_category(self, row: asyncpg.Record) -> Category:
        return Category(
            id=int(row["id"]),
            user_id=int(row["user_id"]),
            family_id=(int(row["family_id"]) if row.get("family_id") is not None else None),
            template_id=(int(row["template_id"]) if row["template_id"] is not None else None),
            type=str(row["type"]),
            name=str(row["name"]),
            slug=normalize_category_slug(row["slug"]) if "slug" in row else None,
            aliases=list(row["aliases"] or []),
            source=str(row["source"] or "custom"),
            is_system=bool(row["is_system"]),
            is_active=bool(row["is_active"]),
            sort_order=int(row["sort_order"] or 0),
            deleted_at=row["deleted_at"],
        )

    async def getUserCategories(self, userId: int, type: CategoryType, includeInactive: bool = False) -> list[Category]:
        scope = await self._scope(userId)
        filters = "family_id=$1 AND type=$2" if scope.is_family else "user_id=$1 AND family_id IS NULL AND type=$2"
        if not includeInactive:
            filters += " AND is_active=true"
        order_by = "is_active DESC, sort_order ASC, name ASC" if includeInactive else "sort_order ASC, name ASC"
        rows = await self._conn.fetch(
            f"""
            SELECT id, user_id, family_id, template_id, type, name, slug, aliases, source, is_system, is_active, sort_order, deleted_at
            FROM categories
            WHERE {filters}
            ORDER BY {order_by}
            """,
            int(scope.family_id) if scope.is_family else int(userId),
            type,
        )
        return [self._map_category(row) for row in rows]

    async def getCategoryById(self, userId: int, categoryId: int) -> Category | None:
        scope = await self._scope(userId)
        if scope.is_family:
            row = await self._conn.fetchrow(
                """
                SELECT id, user_id, family_id, template_id, type, name, slug, aliases, source, is_system, is_active, sort_order, deleted_at
                FROM categories
                WHERE id=$1 AND family_id=$2
                """,
                int(categoryId),
                int(scope.family_id),
            )
        else:
            row = await self._conn.fetchrow(
                """
                SELECT id, user_id, family_id, template_id, type, name, slug, aliases, source, is_system, is_active, sort_order, deleted_at
                FROM categories
                WHERE id=$1 AND user_id=$2 AND family_id IS NULL
                """,
                int(categoryId),
                int(userId),
            )
        return self._map_category(row) if row else None

    async def getCategoryByTemplateId(self, userId: int, templateId: int) -> Category | None:
        scope = await self._scope(userId)
        if scope.is_family:
            row = await self._conn.fetchrow(
                """
                SELECT id, user_id, family_id, template_id, type, name, slug, aliases, source, is_system, is_active, sort_order, deleted_at
                FROM categories
                WHERE family_id=$1 AND template_id=$2
                LIMIT 1
                """,
                int(scope.family_id),
                int(templateId),
            )
        else:
            row = await self._conn.fetchrow(
                """
                SELECT id, user_id, family_id, template_id, type, name, slug, aliases, source, is_system, is_active, sort_order, deleted_at
                FROM categories
                WHERE user_id=$1 AND family_id IS NULL AND template_id=$2
                LIMIT 1
                """,
                int(userId),
                int(templateId),
            )
        return self._map_category(row) if row else None

    async def getCategoryBySlug(self, userId: int, type: CategoryType, slug: str) -> Category | None:
        normalized_slug = normalize_category_slug(slug)
        if normalized_slug is None:
            return None
        scope = await self._scope(userId)
        if scope.is_family:
            row = await self._conn.fetchrow(
                """
                SELECT id, user_id, family_id, template_id, type, name, slug, aliases, source, is_system, is_active, sort_order, deleted_at
                FROM categories
                WHERE family_id=$1
                  AND type=$2
                  AND slug=$3
                LIMIT 1
                """,
                int(scope.family_id),
                type,
                normalized_slug,
            )
        else:
            row = await self._conn.fetchrow(
                """
                SELECT id, user_id, family_id, template_id, type, name, slug, aliases, source, is_system, is_active, sort_order, deleted_at
                FROM categories
                WHERE user_id=$1
                  AND family_id IS NULL
                  AND type=$2
                  AND slug=$3
                LIMIT 1
                """,
                int(userId),
                type,
                normalized_slug,
            )
        return self._map_category(row) if row else None

    async def findCategoryByName(self, userId: int, type: CategoryType, name: str, *, includeInactive: bool = True) -> Category | None:
        target = normalize_category_name(name)
        if not target:
            return None
        for category in await self.getUserCategories(userId, type, includeInactive=includeInactive):
            if _same_name(category.name, target):
                return category
        return None

    async def createCategory(
        self,
        userId: int,
        type: CategoryType,
        name: str,
        aliases: list[str] | None = None,
        source: str = "custom",
        *,
        template_id: int | None = None,
        slug: str | None = None,
        is_system: bool = False,
        sort_order: int | None = None,
    ) -> int:
        scope = await self._scope(userId)
        normalized_name = validate_category_name(name)
        normalized_slug = normalize_category_slug(slug)
        existing = await self.findCategoryByName(userId, type, normalized_name, includeInactive=True)
        if existing:
            raise CategoryConflictError("category already exists")
        if normalized_slug is not None:
            existing_by_slug = await self.getCategoryBySlug(userId, type, normalized_slug)
            if existing_by_slug is not None:
                raise CategoryConflictError("category slug already exists")
        normalized_aliases = _norm_aliases(aliases)
        if sort_order is None:
            if scope.is_family:
                next_sort = await self._conn.fetchval(
                    """
                    SELECT COALESCE(max(sort_order), 0) + 10
                    FROM categories
                    WHERE family_id=$1
                      AND type=$2
                    """,
                    int(scope.family_id),
                    type,
                )
            else:
                next_sort = await self._conn.fetchval(
                    """
                    SELECT COALESCE(max(sort_order), 0) + 10
                    FROM categories
                    WHERE user_id=$1
                      AND family_id IS NULL
                      AND type=$2
                    """,
                    int(userId),
                    type,
                )
            sort_order = int(next_sort or 10)
        row = await self._conn.fetchrow(
            """
            INSERT INTO categories (
              tg_user_id, kind, user_id, family_id, template_id, type, name,
              aliases, source, is_system, is_active, sort_order, slug,
              created_by_user_id, created_at, updated_at
            )
            VALUES ($1, $2, $1, $3, $4, $2, $5, $6, $7, $8, true, $9, $10, $1, now(), now())
            RETURNING id
            """,
            int(userId),
            type,
            int(scope.family_id) if scope.is_family else None,
            int(template_id) if template_id is not None else None,
            normalized_name,
            normalized_aliases,
            source,
            bool(is_system),
            int(sort_order),
            normalized_slug,
        )
        return int(row["id"])

    async def renameCategory(self, userId: int, categoryId: int, newName: str) -> Category:
        category = await self._require_active_category(userId, categoryId)
        if is_fixed_default_expense_category(category):
            raise CategoryValidationError("default expense categories are fixed")
        normalized_name = validate_category_name(newName)
        if _same_name(category.name, normalized_name):
            raise CategoryValidationError("category name is unchanged")
        existing = await self.findCategoryByName(userId, category.type, normalized_name, includeInactive=False)
        if existing and existing.id != categoryId:
            raise CategoryConflictError("category already exists")
        await self._conn.execute(
            """
            UPDATE categories
            SET name=$2, updated_at=now()
            WHERE id=$1
            """,
            int(categoryId),
            normalized_name,
        )
        renamed = await self.getCategoryById(userId, categoryId)
        if renamed is None:
            raise CategoryUnavailableError("category is unavailable")
        return renamed

    async def archiveCategory(self, userId: int, categoryId: int) -> Category:
        category = await self._require_active_category(userId, categoryId)
        if is_fixed_default_expense_category(category):
            raise CategoryValidationError("default expense categories are fixed")
        await self._conn.execute(
            """
            UPDATE categories
            SET is_active=false, deleted_at=now(), updated_at=now()
            WHERE id=$1
            """,
            int(categoryId),
        )
        archived = await self.getCategoryById(userId, categoryId)
        if archived is None:
            raise CategoryUnavailableError("category is unavailable")
        return archived

    async def restoreCategory(self, userId: int, categoryId: int) -> None:
        await self._conn.execute(
            """
            UPDATE categories
            SET is_active=true, deleted_at=NULL, updated_at=now()
            WHERE id=$1
            """,
            int(categoryId),
        )

    async def addCategoryAliases(self, userId: int, categoryId: int, aliases: list[str]) -> None:
        category = await self._require_active_category(userId, categoryId)
        if is_fixed_default_expense_category(category):
            raise CategoryValidationError("default expense categories are fixed")
        await self._conn.execute(
            """
            UPDATE categories
            SET aliases=$2, updated_at=now()
            WHERE id=$1
            """,
            int(categoryId),
            _norm_aliases(aliases),
        )

    async def ensureSystemCategories(self, userId: int) -> None:
        await self.ensureDefaultExpenseCategories(userId)
        await self._ensure_system_income_category(userId)

    async def ensureDefaultIncomeCategories(self, userId: int, *, reactivate_inactive: bool = False) -> None:
        templates = await CategoryTemplateService(self._conn).syncDefaultIncomeTemplates()
        await self._ensure_templates(userId, "income", templates, reactivate_inactive=reactivate_inactive)
        await self._deactivate_stale_default_income_categories(userId)

    async def ensureDefaultExpenseCategories(self, userId: int) -> None:
        template_service = CategoryTemplateService(self._conn)
        templates = await template_service.syncDefaultExpenseTemplates()
        template_by_slug = {normalize_category_slug(template.slug): template for template in templates}
        for definition in DEFAULT_EXPENSE_CATEGORIES:
            slug = str(definition["slug"])
            template = template_by_slug.get(slug)
            if template is None:
                continue
            await self._ensure_default_expense_category(userId, definition, template)

    async def getFallbackExpenseCategory(self, userId: int) -> Category:
        await self.ensureDefaultExpenseCategories(userId)
        fallback = await self.getCategoryBySlug(userId, "expense", DEFAULT_EXPENSE_FALLBACK_SLUG)
        if fallback is None:
            raise CategoryUnavailableError("fallback expense category is unavailable")
        return fallback

    async def restoreDefaultCategories(self, userId: int) -> None:
        await self.ensureDefaultExpenseCategories(userId)
        await self.ensureDefaultIncomeCategories(userId, reactivate_inactive=True)

    async def ensureDefaultCategories(self, userId: int) -> None:
        await self.ensureDefaultExpenseCategories(userId)
        await self.ensureDefaultIncomeCategories(userId, reactivate_inactive=False)

    async def createCategoriesFromOnboardingSelection(self, userId: int, selection: dict[str, Any]) -> None:
        _ = selection
        await self.ensureDefaultCategories(userId)

    async def _ensure_system_income_category(self, userId: int) -> None:
        await self._ensure_template_by_name(
            userId,
            "income",
            SYSTEM_INCOME_NAME,
            reactivate_inactive=True,
            source="system",
            is_system=True,
            slug=None,
            aliases=[],
            sort_order=50,
        )

    async def _deactivate_stale_default_income_categories(self, userId: int) -> None:
        scope = await self._scope(userId)
        stale_names = list(STALE_DEFAULT_INCOME_CATEGORY_NAMES)
        if scope.is_family:
            await self._conn.execute(
                """
                UPDATE categories
                SET is_active=false,
                    deleted_at=COALESCE(deleted_at, now()),
                    updated_at=now()
                WHERE family_id=$1
                  AND type='income'
                  AND is_active=true
                  AND source IN ('system', 'template')
                  AND (
                    name = ANY($2::text[])
                    OR template_id IN (
                      SELECT id FROM category_templates
                      WHERE type='income' AND is_active=false
                    )
                  )
                """,
                int(scope.family_id),
                stale_names,
            )
            return
        await self._conn.execute(
            """
            UPDATE categories
            SET is_active=false,
                deleted_at=COALESCE(deleted_at, now()),
                updated_at=now()
            WHERE user_id=$1
              AND family_id IS NULL
              AND type='income'
              AND is_active=true
              AND source IN ('system', 'template')
              AND (
                name = ANY($2::text[])
                OR template_id IN (
                  SELECT id FROM category_templates
                  WHERE type='income' AND is_active=false
                )
              )
            """,
            int(userId),
            stale_names,
        )

    async def _ensure_templates(
        self,
        userId: int,
        type: CategoryType,
        templates: list[CategoryTemplate],
        *,
        reactivate_inactive: bool,
    ) -> None:
        for template in templates:
            await self._ensure_template(userId, type, template, reactivate_inactive=reactivate_inactive)

    async def _ensure_template(
        self,
        userId: int,
        type: CategoryType,
        template: CategoryTemplate,
        *,
        reactivate_inactive: bool,
    ) -> None:
        await self._ensure_template_by_name(
            userId,
            type,
            template.name,
            reactivate_inactive=reactivate_inactive,
            source="system" if template.is_system else "template",
            template_id=template.id,
            slug=template.slug,
            is_system=template.is_system,
            aliases=list(template.aliases or []),
            sort_order=template.sort_order,
        )

    async def _ensure_default_expense_category(
        self,
        userId: int,
        definition: dict[str, object],
        template: CategoryTemplate,
    ) -> None:
        slug = str(definition["slug"])
        name = str(definition["name"])
        aliases = _norm_aliases(list(definition.get("aliases") or []))
        sort_order = int(definition["sort_order"])
        is_system = slug == DEFAULT_EXPENSE_FALLBACK_SLUG
        existing = await self.getCategoryBySlug(userId, "expense", slug)
        if existing is None:
            existing = await self.getCategoryByTemplateId(userId, template.id)
        if existing is None:
            existing = await self.findCategoryByName(userId, "expense", name, includeInactive=True)

        if existing is not None:
            await self._conn.execute(
                """
                UPDATE categories
                SET name=$2,
                    slug=$3,
                    template_id=$4,
                    aliases=$5,
                    source=$6,
                    is_system=$7,
                    is_active=true,
                    sort_order=$8,
                    deleted_at=NULL,
                    updated_at=now()
                WHERE id=$1
                """,
                int(existing.id),
                name,
                slug,
                int(template.id),
                aliases,
                "system" if is_system else "template",
                is_system,
                sort_order,
            )
            return

        await self.createCategory(
            userId=userId,
            type="expense",
            name=name,
            slug=slug,
            aliases=aliases,
            source="system" if is_system else "template",
            template_id=template.id,
            is_system=is_system,
            sort_order=sort_order,
        )

    async def _ensure_template_by_name(
        self,
        userId: int,
        type: CategoryType,
        name: str,
        *,
        reactivate_inactive: bool,
        source: str,
        template_id: int | None = None,
        slug: str | None = None,
        is_system: bool = False,
        aliases: list[str] | None = None,
        sort_order: int | None = None,
    ) -> None:
        normalized_slug = normalize_category_slug(slug)
        existing = None
        if normalized_slug is not None:
            existing = await self.getCategoryBySlug(userId, type, normalized_slug)
        if existing is None and template_id is not None:
            existing = await self.getCategoryByTemplateId(userId, template_id)
        if existing is None:
            existing = await self.findCategoryByName(userId, type, name, includeInactive=True)
        if existing is not None:
            if reactivate_inactive or existing.is_active:
                await self._conn.execute(
                    """
                    UPDATE categories
                    SET template_id=COALESCE($2, template_id),
                        slug=COALESCE($3, slug),
                        source=CASE WHEN source='custom' AND $4 <> 'system' THEN source ELSE $4 END,
                        is_system=CASE WHEN $5 THEN true ELSE is_system END,
                        aliases=CASE WHEN cardinality(aliases) = 0 THEN $6 ELSE aliases END,
                        sort_order=CASE WHEN sort_order = 0 THEN $7 ELSE sort_order END,
                        is_active=CASE WHEN $8 THEN true ELSE is_active END,
                        deleted_at=CASE WHEN $8 THEN NULL ELSE deleted_at END,
                        updated_at=now()
                    WHERE id=$1
                    """,
                    int(existing.id),
                    int(template_id) if template_id is not None else None,
                    normalized_slug,
                    source,
                    bool(is_system),
                    _norm_aliases(aliases),
                    int(sort_order) if sort_order is not None else 0,
                    bool(reactivate_inactive),
                )
            return
        await self.createCategory(
            userId=userId,
            type=type,
            name=name,
            slug=normalized_slug,
            aliases=aliases or [],
            source=source,
            template_id=template_id,
            is_system=is_system,
            sort_order=sort_order,
        )

    async def _require_active_category(self, userId: int, categoryId: int) -> Category:
        category = await self.getCategoryById(userId, categoryId)
        if category is None or not category.is_active:
            raise CategoryUnavailableError("category is unavailable")
        return category
