# HEALTH-05 — звичайна операція: контракт захисного acceptance

[Почати тут](README.md) · [Рішення та межі](DECISIONS.md) · [Карта впливу](FLOW_MAP.md)

**Статус: PREPARED, NOT EXECUTED.** Цей docs-only PR визначає наступний технічний slice, але не додає executable feature suite і не доводить роботу фінансового сценарію. Runtime, БД продукту й production не змінено. Реалізація тестового маршруту — наступна окрема задача; PR і `validate` цього baseline не закривають `HEALTH-05`.

## Мета і scope

Користувач у PWA створює звичайну витрату або дохід у UAH: **draft → preview → explicit confirm → persisted transaction → balance/history/activity/export readback**. Повторення confirm не має другого фінансового ефекту; cancel до confirm не пише ledger; void зберігає audit history та не повторює reversal. Під звітним readback нижче маються на увазі наявні history/activity/export за однаковим scope/періодом, не вигаданий endpoint `/report`.

- Перший acceptance — manual normal expense/income, personal scope, власний звичайний рахунок, одна базова валюта UAH і достатній баланс для витрати. Дати/період звіту та application timezone фіксуються у fixture.
- Інваріанти: `INV-CONFIRM`, `INV-ONCE`, `INV-SCOPE`; фактичні balance/history/report значення перевіряються проти стану до операції, не тільки HTTP status або тексту toast.
- Вхід: baseline `HEALTH-00` переглянуто/смeржено, scope і test fixtures відомі; новий delivery contract містить цю вимогу та реальний acceptance command.
- Не входять: FX/transfer (`HEALTH-06`), повна family матриця (`HEALTH-07`), text/photo/voice, debt, billing/provider, Telegram фінансовий UI, domain refactor (`HEALTH-09`). Вони не стають «перевіреними» через цей slice.
- Partner/card рішення не змінюємо: fixture заздалегідь дає користувачу валідний доступ. Локальна test identity допустима лише з явною позначкою **synthetic auth**, без claims про real Telegram auth чи production access.

## Acceptance matrix

| ID | Дія | Обов’язкове спостереження |
|---|---|---|
| SCN-TX-01 | Відкрити manual expense, ввести дані, переглянути preview; ще не confirm | Немає нового фінансового запису чи зміни балансу; UI відображає тип, суму, валюту, рахунок, категорію, дату й опис для підтвердження |
| SCN-TX-02 | Підтвердити витрату через PWA | Рівно один durable ефект; правильний owner/account/category/source; баланс зменшений на суму, історія й звіт за тим самим scope/періодом відповідають DB readback; reload не губить запис |
| SCN-TX-03 | Повторити окремо для доходу | Баланс і звіт змінюються у правильному напрямку; дохід не витрата/переказ; expense fixture не маскує income поведінку |
| SCN-TX-04 | Викликати наявний draft cancel; окремо закрити preview без confirm і повторити з reload | Немає ledger/balance/report ефекту. Explicit API cancel і UI abandon перевіряються окремо; не припускати, що закриття UI саме викликає cancel endpoint |
| SCN-TX-05 | Replay того самого confirm, потім concurrent confirm з тим самим draft/receipt | DB cardinality та чистий balance delta лишаються одиничними. Додатковий request може бути idempotent result або очікуваним conflict; HTTP 200 сам по собі нічого не доводить |
| SCN-TX-06 | Інший ізольований user намагається confirm/read чужого draft/record | Відмова згідно з чинним контрактом; немає чужого readback або фінансової мутації. Це один personal ownership guard, не повна family acceptance |
| SCN-TX-07 | Через наявний PWA шлях виконати void draft → preview → confirm підтвердженого запису; повторити | Запис позначений скасованим зі збереженою історією; початковий ефект відшкодовано один раз; баланс/історія/звіт узгоджені після reload. Окремо void cancel не змінює ledger. API route існує; browser entrypoint/селектори ще потребують перевірки |
| SCN-TX-08 | Неповна/невалідна операція та expired/stale confirm, де цей стан підтриманий | Зрозуміла UI/API помилка; жодної часткової фінансової зміни. Failure injection не підміняє реальний SQL commit, а missing capability позначається окремо |

Не встановлювати нове правило поведінки тестом без review. Якщо фактичний продукт і погоджені вимоги розходяться — зберегти спостереження, відокремити defect від policy question, отримати рішення і лише тоді міняти runtime.

## Перевірені маршрути та наявні test seams

Ці prerequisite anchors звірено з кодом publication base `77d3600364856583ff0da6ebc9becc912f16a699`, а не перенесено з audit snapshot без перевірки. Перед реалізацією повторити звірку на новому candidate.

| Сценарій | Наявне визначення / межа доказу |
|---|---|
| Manual expense/income | `POST /app/api/transactions/draft`, `/confirm`, `/cancel`; `admin_service/miniapp/urls.py:89–91`, `admin_service/miniapp/views.py:2222–2244`, `:2430–2503`. Confirm використовує `draft_id`, `idempotency_key` і durable `_commit_draft_once`; `kind` розрізняє expense/income |
| Readback | `GET /app/api/accounts`, `/history`, `/activity`, `/export.csv`, `/export.xlsx`; `admin_service/miniapp/urls.py:112–122`. Окремого route `/report` у цій URL configuration немає; payload/фільтри й UI ще потрібно простежити до readback |
| Void | `POST /app/api/history/void/draft`, `/confirm`, `/cancel`; `admin_service/miniapp/urls.py:117–119`, `admin_service/miniapp/views.py:3107–3173`. Це підтверджує API, не проходження UI |
| PostgreSQL characterization | `admin_service/miniapp/tests_mini_confirmation_postgres.py:25`, `:96–140`, `:190–205`, `:234–289`: gate `MINI_CONFIRM_FULL_FINANCE_TESTS`, replay/concurrency/cancel та history edit/void. Наявність тестів не означає, що вони виконані |
| Isolated integrity probes | `admin_service/miniapp/tests_mini_confirmation_integrity.py:1–5`, `:28–38`: gate `MINI_CONFIRM_ISOLATED_TESTS` і окрема test schema. Не запускати через випадковий успадкований DB connection |
| Existing isolation runner | `ops/browser_acceptance_smoke.py:63–99`, `:131–138`: disposable PostgreSQL container, очищення inherited `DATABASE_URL`, loopback port і teardown. `docs/BROWSER_ACCEPTANCE.md:14–16` прямо обмежує smoke shell/onboarding/navigation, без фінансових дій |

Наявні documented entrypoints: `python ops/browser_acceptance_smoke.py --preflight` та `python ops/browser_acceptance_smoke.py --server --log .hermes/delivery/<session-id>/pwa-browser.log`. Вони **не є acceptance command для HEALTH-05**. У prerequisite review не встановлено готової відтворюваної команди, що безпечно створює finance/probe schema, задає обидва test gates і проходить цю browser matrix. До появи й фактичної перевірки такого runner статус executable acceptance — **BLOCKED / NOT EXECUTED**. Не підміняти runner ручним виставленням flags проти невідомої БД.

## Capability preflight — до реалізації

| Потрібно | Умова допуску / BLOCKED |
|---|---|
| PostgreSQL | Окремий disposable кластер/БД для suite; перевірений guard на loopback/test DB; ніколи не використовувати конфігурацію product/production in place |
| Django schema | Міграції й fixture із кодової ревізії кандидата; не SQLite і не mocked ledger для фінансового доказу |
| PWA browser | Headless Chromium/Edge/Playwright запускається; реальні PWA UI дії досягають реального test backend; static shell не є цим маршрутом |
| Identity/access | Test-only login/session в ізольованому середовищі; явно вказати synthetic auth. Production identity/provider flows поза scope |
| Дані та облік | Відомі accounts/categories/owner, timezone/period, початковий ledger і balances; очікування сум через Decimal/код, без guessed totals |
| Ізоляція | Паралельні suites мають різні DB/schema/user IDs; cleanup тільки власного test namespace, safe teardown після failure |
| Спостереження | Browser, API та DB readback зв’язані одним test run ID; account/transaction/draft/receipt IDs санітизовані, без PII, cookie чи credential values |

Якщо capability немає, саме відповідний рядок лишається **BLOCKED**. Можна окремо запустити component tests, але їхній PASS не переводить browser/PostgreSQL acceptance у PASS.

## Послідовність реалізації наступної задачі

1. Порівняти актуальні `origin/main`, exact production SHA і source candidate; почати окрему task-гілку від main. Не змішувати з audit-документами нові product fixes.
2. Запустити `delivery.py start` з повною вимогою, `--kind feature` для нового захисного suite, `--target local`, requirement на кожну спостережувану групу й точним executable acceptance command. Не вигадувати команду до появи реалізації; потім preflight.
3. Перевірити поточні URL → views → services → commit/readback на реальній схемі. Використати наявні draft/receipt/idempotency seams; не мокати фінансовий commit.
4. Реалізувати один reproducible isolated runner. Characterization не зобов’язана штучно ламати продукт, щоб отримати RED. Кожен виявлений defect оформити окремим bugfix contract, відтворити і зберегти справжнє RED до виправлення причини.
5. Пройти matrix у browser + PostgreSQL і повторний чистий запуск без залишків fixtures. Для concurrency перевірити реальну competing DB write, а не два виклики mock.
6. Snapshot/preflight фінального кандидата → sanitized logs → requirement-to-evidence links → незалежний review точного кандидата → readiness. Далі звичайний PR/required CI; deployment тільки якщо окремий погоджений runtime change його потребує.

## Evidence і definition of done

Кожна група вимог має: backlog `HEALTH-05`, delivery task ID і requirement IDs; full source candidate/fingerprint; точну команду, exit code, test run ID; ізольоване середовище/тип auth; початковий/кінцевий readback, browser failures/screenshots або trace за потреби; verdict незалежного reviewer-а.

**DONE — LOCAL** лише якщо всі погоджені matrix rows пройдені на фінальному candidate, named runner реально виконаний, повторний чистий запуск відтворюваний, усі вимоги мають evidence і немає прихованих skip/mock замість PostgreSQL/browser. GAP/BLOCKED і `not executed` не перефарбовуються в passed. Наступний executable receipt gate `delivery-verification`, якщо він недоступний, лишається окремим явно названим delivery blocker.

**Не означає DONE:** наявність цього документа, збільшення test count, green shell smoke, створений PR, production HTTP 200 чи локальна перевірка audit dashboard.
