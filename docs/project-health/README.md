# Здоров’я проєкту: почати тут

**Аудит 19 жовтня 2026. Docs-only PR candidate, не оголошення готовності продукту.**

Це стартова панель для людини й агента: що відомо, де ризик, яке рішення потрібне і що робити наступним. Поточний код і правила production не змінено. Наявні `delivery.py`, `validate` і exact-SHA deployment залишаються основою.

## Відкрити

- **[Людський dashboard](dashboard.html)** — самодостатня HTML-сторінка, відкривається локально без сервера; фільтри сценаріїв/беклогу й рішення. Це snapshot, не live GitHub Project.
- **[План поетапних змін](ROADMAP.md)** — послідовність пакетів та критерії виходу.
- **[Рекомендовані рішення](DECISIONS.md)** — конкретні defaults, погоджений scope та відкриті бізнес-рішення.
- **[Наступний технічний slice HEALTH-05](HEALTH-05.md)** — обмежений контракт PWA/PostgreSQL acceptance; ще не виконаний.
- **[Карта сценаріїв та впливу](FLOW_MAP.md)** — фінансовий запис, доступ, фонові ефекти й acceptance matrix.
- **[Знахідки та межі доказів](FINDINGS.md)** — факти окремо від ризиків, гіпотез і неперевіреного.
- **[Робоча модель команди](OPERATING_MODEL.md)** — ролі, WIP, передача задач, джерела істини; пропозиція до погодження.
- [Машинний реєстр](register.json) — ID, інваріанти, сценарії, рішення, залежності й початковий беклог.
- [Статичний inventory](inventory.json) — назви конфігурації та місця використання, Django routes, Telegram handlers, task decorators. Без значень конфігурації.
- [Git/GitHub/production baseline](baseline.json) — перевірені revision/артефакти/захист гілки та санітизовані HTTP-результати.

## Короткий висновок

Проєкт не «без документації й пайплайнів»: вони є, але не з’єднані в надійну систему приймання. Головний розрив — **вимога → сценарій → доказ → реліз → приймання людиною**. Рефакторинг без попереднього захисту сценаріїв лише перемістить невизначеність.

Ключові технічні ризики: два фінансові write-path (PWA/Django і bot/asyncpg), складні права personal/family, повторні confirm/FX/void, великі orchestration-модулі. Це не доказ, що всі ці маршрути зараз несправні.

## Стан бази

- Canonical audit/production artifact SHA: `f2e53b659cee68da7d1cfb6ebabd89384fe180e2`; точні Git/GitHub timestamps — у baseline.
- Гілку публікації оновлено до `origin/main` `77d3600364856583ff0da6ebc9becc912f16a699` перед змінами. Новий access/billing fix у main не переносить старі audit anchors на новий SHA: inventory та висновки лишаються датованим snapshot, а не повторним аудитом цього fix. На повторній перевірці останній successful production run лишався `39158608725` на audit SHA.
- У первинній робочій копії була recovery-гілка, яка розходиться з `origin/main`; аудит виконано окремо від актуального main. Локальні незавершені роботи не скинуто й не приєднано wholesale.
- Main protection в момент перевірки: required `validate`, admin enforcement; **0 required approvals**. Правила не змінено.
- Production `/healthz`, `/readyz`, `/app/` дали HTTP 200. Це доступність, не authenticated business acceptance і не незалежна runtime-attestation всіх контейнерів.
- Усі end-to-end acceptance-статуси стартового реєстру — **UNVERIFIED у межах цього аудиту**. Раніше наявні component tests не оголошуються зниклими; вони просто не були повторно виконані тут.

## Що оцифровано

| Об’єкт | Кількість | Межа |
|---|---:|---|
| Продуктові сценарії | 18 | Початкова карта; не доказ повної інвентаризації всього продукту |
| Фінансові/безпекові інваріанти | 9 | Контракти до уточнення по сценаріях |
| Знахідки | 10 | Fact/risk/gap/hypothesis явно розділені |
| Початкові задачі | 16 | Не створені GitHub Issues і не реалізовані зміни |
| Рішення власника | 4 | Три продуктові/організаційні; одне про review enforcement |
| Статичні config names | 103 | Лише literal reads; не live config, без значень |
| Django route declarations | 133 | Не resolved API coverage; includes/nested prefixes потребують перевірки |
| Telegram handler declarations | 52 | Не повна FSM/callback state map |
| Task decorators | 17 | Не весь scheduler/queue dependency graph |

Inventory отримано AST-аналізом tracked runtime Python на audit SHA, без читання `.env`/credential-файлів. Literal config helper calls враховано; dynamic keys, compose-only names, shell references і власники ще неповні. Це видимий борг `HEALTH-04/HEALTH-12`, а не вигадане «інвентаризовано все».

## Що робити першим

1. **Використати конкретні рекомендації:** [DEC-01–DEC-04](DECISIONS.md). Не змінювати partner/card поведінку, не прибирати bot capabilities, вести контракти в Git; незалежний огляд без зміни protection. Продуктовий вибір cardless та capability-by-capability disposition лишаються відкритими.
2. **Опублікувати docs-only baseline** через звичайний PR/CI — це поточний дозволений крок. Публікація та merge перевіряються окремо в GitHub, не виводяться зі слова «готово» у snapshot. Автоматичний merge і production deploy не входять у цей scope.
3. **Один вертикальний acceptance `HEALTH-05`:** draft → confirm → balance/history/report → duplicate/cancel/void, із реальною ізольованою PostgreSQL і PWA. Потім transfer/FX `HEALTH-06` та family `HEALTH-07`.
4. Лише після characterization обрати domain seam `HEALTH-09`. Не переписувати великі файли всі разом.

Ролі — функції, не постійний штат: інтегратор, аналітик вимог/домену, implementer, незалежний QA/reviewer, UX за потреби, release owner. Одна активна реалізація спільного домену; незалежні читання й рев’ю паралельно. Потужну модель використовувати для ризику, а не механічної роботи.

## Перевірка цього пакета

З кореня репозиторію:

`python docs/project-health/check.py`

Перевіряє локальні посилання, ID/cross-references/dependencies, існування source anchors, санітизовану структуру inventory, відсутність бездоказово зелених acceptance. Anchors належать саме audit SHA; для перегляду джерела використовуйте `git show <audit-sha>:<path>`, а не поточний рядок зміненого main. **Не запускає продуктову acceptance matrix**. `python docs/project-health/check.py --browser` перевіряє лише локальний read-only dashboard; `browser.json` не є доказом фінансового PWA/provider/production acceptance. Evidence зберігається локально, не є release input.

Після публікації: реальні активні статуси — у погодженому GitHub backlog, не в паралельних ручних копіях. Snapshot залишається датованим. Документи зовнішнього vault не скасовані цим пакетом; міграція — окрема погоджена задача.
