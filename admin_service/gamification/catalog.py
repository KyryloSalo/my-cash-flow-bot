from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AchievementDefinition:
    key: str
    name: str
    description: str
    condition: str
    category: str
    scene_numbers: tuple[int, ...]
    scene_slugs: tuple[str, ...]
    thresholds: tuple[int, ...] = ()
    hint: str | None = None
    secret: bool = False
    availability: str = "unavailable"


def _definition(
    key: str,
    name: str,
    description: str,
    condition: str,
    category: str,
    scene_numbers: tuple[int, ...],
    scene_slugs: tuple[str, ...],
    *,
    thresholds: tuple[int, ...] = (),
    hint: str | None = None,
    secret: bool = False,
    available: bool = False,
) -> AchievementDefinition:
    return AchievementDefinition(
        key=key,
        name=name,
        description=description,
        condition=condition,
        category=category,
        scene_numbers=scene_numbers,
        scene_slugs=scene_slugs,
        thresholds=thresholds,
        hint=hint,
        secret=secret,
        availability="available" if available else "unavailable",
    )


_DEFINITIONS = (
    _definition("first-record", "Початок видно", "Перша підтверджена операція у Vydno.", "Додайте першу кваліфіковану витрату або дохід.", "start", (3,), ("first-record",), available=True),
    _definition("first-voice", "Алло, це бюджет", "Перша операція, записана голосом.", "Підтвердьте кваліфіковану операцію з voice.", "start", (4,), ("first-voice",), available=True),
    _definition("first-text", "Два слова — і готово", "Перша операція, розпізнана з тексту.", "Підтвердьте кваліфіковану операцію з text.", "start", (5,), ("first-text",), available=True),
    _definition("first-screenshot", "Скрін усе пам’ятає", "Перша операція зі скріншота.", "Підтвердьте кваліфіковану операцію зі screenshot.", "start", (6,), ("first-screenshot",), available=True),
    _definition(
        "streak-3",
        "В ритмі",
        "Послідовні активні дні.",
        "Підтримуйте особисту серію активних днів.",
        "habit",
        (10, 11, 12, 13, 14, 15, 16, 17),
        ("streak-3", "streak-7", "streak-14", "streak-30", "streak-60", "streak-100", "streak-180", "streak-365"),
        thresholds=(3, 7, 14, 30, 60, 100, 180, 365),
        available=True,
    ),
    _definition("lifetime-days", "Досвід не зникає", "Активні дні накопичуються назавжди.", "Досягніть 30, 100 і 365 активних днів.", "habit", (19,), ("lifetime-days",), thresholds=(30, 100, 365), available=True),
    _definition("voice-days", "Голос бюджету", "Голосові записи в різні дні.", "Додавайте voice-операції у 5, 20 і 50 різних активних днів.", "habit", (20,), ("voice-days",), thresholds=(5, 20, 50), available=True),
    _definition("screenshot-days", "Око-алмаз", "Скріншоти в різні дні.", "Додавайте screenshot-операції у 5, 20 і 50 різних активних днів.", "habit", (21,), ("screenshot-days",), thresholds=(5, 20, 50), available=True),
    _definition("categorized", "Усе по поличках", "Усі витрати закритого тижня мають категорії.", "Підтвердьте повністю категоризований тижневий огляд.", "order", (22,), ("categorized",)),
    _definition("weekly-review", "Фінансове побачення", "Регулярні тижневі огляди.", "Завершіть 1, 4 і 12 різних закритих тижнів.", "order", (23,), ("weekly-review",), thresholds=(1, 4, 12)),
    _definition("monthly-review", "Місяць видно", "Повний огляд активного місяця.", "Завершіть огляд закритого місяця з 20 активними днями.", "order", (24,), ("monthly-review",)),
    _definition("debt-cleared", "Мінус один", "Реально погашений борг.", "Повністю погасіть додатний борг записами погашень.", "order", (25,), ("debt-cleared",)),
    _definition("goal-created", "Є заради чого", "Перша фінансова ціль.", "Створіть ціль із додатною цільовою сумою.", "goals", (26,), ("goal-created",)),
    _definition("goal-first-funding", "Перший крок", "Перший внесок у ціль.", "Зробіть перший додатний підтверджений внесок у ціль.", "goals", (27,), ("goal-first-funding",)),
    _definition("goal-progress", "Уже ближче", "Прогрес особистої цілі.", "Досягніть 25%, 50% і 75% чистими внесками.", "goals", (28,), ("goal-progress",), thresholds=(25, 50, 75)),
    _definition("goal-completed", "Мрія здійснилася", "Особиста ціль профінансована.", "Досягніть 100% особистої цілі внесками.", "goals", (29,), ("goal-completed",)),
    _definition("family-joined", "Тепер разом", "Активна сім’я у Vydno.", "Прийміть сімейне запрошення і залиштеся в активній сім’ї.", "family", (30,), ("family-joined",)),
    _definition("family-contributors", "Обоє у справі", "Обидва учасники ведуть спільний бюджет.", "Додайте операції обома учасниками за один сімейний тиждень.", "family", (31,), ("family-contributors",)),
    _definition("family-streak", "На одній хвилі", "Серія погодженої сімейної пари.", "Досягніть сімейної серії 3, 7 і 30 днів.", "family", (32,), ("family-streak",), thresholds=(3, 7, 30)),
    _definition("family-goal", "Спільна перемога", "Спільна ціль профінансована разом.", "Завершіть сімейну ціль внесками щонайменше двох учасників.", "family", (33,), ("family-goal",)),
    _definition("category-discovery", "Так ось куди!", "Знайдено найбільшу категорію витрат у місячному звіті.", "Відкрийте найбільшу категорію після семи активних днів.", "secret", (34,), ("category-discovery",), hint="У звітах ховаються відповіді", secret=True),
    _definition("seven-fridays", "Сім п’ятниць", "Сім активних п’ятниць.", "Накопичте сім різних активних п’ятниць.", "secret", (35,), ("seven-fridays",), hint="У п’ятниці є своя магія", secret=True, available=True),
    _definition("all-input-methods", "Повний комплект", "Усі способи додавання випробувані.", "Підтвердьте manual, text, voice і screenshot операції.", "secret", (36,), ("all-input-methods",), hint="Спробуй різні шляхи", secret=True, available=True),
    _definition("comeback", "Камбек", "Серію відновлено після справжньої паузи.", "Після розриву й трьох неактивних днів проведіть три active дні поспіль.", "secret", (18,), ("comeback",), hint="Повернення теж варте нагороди", secret=True, available=True),
)

_BY_KEY = {definition.key: definition for definition in _DEFINITIONS}


def achievement_definitions() -> tuple[AchievementDefinition, ...]:
    return _DEFINITIONS


def achievement_definition(key: str) -> AchievementDefinition:
    return _BY_KEY[key]


def public_achievement(
    key: str,
    *,
    mascot: str,
    earned: bool,
    level: int | None = None,
) -> dict[str, object]:
    definition = achievement_definition(key)
    selected_mascot = mascot if mascot in {"bob", "capi"} else "bob"
    scene_index = 0
    if level is not None and level in definition.thresholds and len(definition.scene_numbers) > 1:
        scene_index = definition.thresholds.index(level)
    scene_number = definition.scene_numbers[scene_index]
    scene_slug = definition.scene_slugs[scene_index]
    filename = f"{scene_number:02d}-{scene_slug}"
    prefix = f"/static/miniapp/achievements/assets/{selected_mascot}"
    secret_locked = definition.secret and not earned
    payload: dict[str, object] = {
        "key": definition.key,
        "name": "Секретне досягнення" if secret_locked else definition.name,
        "description": None if secret_locked else definition.description,
        "hint": definition.hint,
        "category": definition.category,
        "is_secret": definition.secret,
        "availability": definition.availability,
        "thresholds": list(definition.thresholds),
        "asset_color": f"{prefix}/{filename}.png",
        "asset_locked": f"{prefix}/{filename}-locked.png",
        "asset_gif": f"{prefix}/{filename}.gif",
    }
    if not secret_locked:
        payload["condition"] = definition.condition
    return payload
