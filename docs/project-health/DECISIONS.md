# Рішення для першого кроку

[Почати тут](README.md) · [Машинний реєстр](register.json) · [Наступний acceptance](HEALTH-05.md)

**19 жовтня 2026. Scope цього кроку: docs-only PR, а не зміна бізнес-правил чи production.** Підстава: доручення власника «Давай наступний крок з запропонованими рышеннями». Воно дозволяє підготувати й опублікувати baseline та рекомендації; не є окремим схваленням cardless trial, видалення bot capabilities або зміни branch protection.

## DEC-01 — Partner trial: зберегти чинну поведінку, не переносити recovery-політику

**Статус: рекомендація; остаточний продуктовий вибір відкритий.** Власник: Product owner.

- Для baseline джерело фактичної поведінки — перевірений production SHA, а не recovery checkout. На audit SHA `f2e53b659cee68da7d1cfb6ebabd89384fe180e2` partner claim резервується, а trial-доступ потребує card binding (`admin_service/miniapp/partner_offers.py:50–51`).
- Рекомендований безпечний default: **не змінювати цей контракт у стабілізаційному пакеті**. Окремо описати acquisition/frozen offer → reservation → activation → access, повторний login, existing user та сильніший доступ.
- Обіцянка «trial без картки» в recovery-документі не є дозволом увімкнути іншу поведінку. Не переносити цей код або маркетинговий текст wholesale.
- Якщо власник обирає cardless trial, це новий явний change request: строки/дні, existing/new identity, family, повторний grant, paid/manual/lifetime precedence, тексти CTA та acceptance на реальному auth/provider-маршруті.

**Що ще вирішити:** чинний card-bound trial залишається продуктовим контрактом чи власник замовляє cardless зміну? До цього рішення `HEALTH-02` не закритий; `HEALTH-05` використовує заздалегідь авторизованого test user і не перевіряє billing.

## DEC-02 — PWA primary; compatibility спочатку інвентаризувати

**Статус: напрям прийнятий для baseline; capability-by-capability disposition відкритий.** Власник: Product owner + domain reviewer.

- PWA — основний фінансовий інтерфейс; Telegram — identity, повідомлення й доставка результатів. Це вже правило проєкту, не нова міграція.
- Нові фінансові UI-сценарії проєктувати для PWA. Для bot спочатку скласти `HEALTH-03`: capability, entrypoint, фактична поведінка, parity gap, owner та `supported / redirect-to-PWA / deprecated`.
- **Нічого не вимикати й не перенаправляти цим PR.** До окремого погодження зберігати поточну сумісність. Deprecated означає рішення з migration notice і acceptance, не мовчазне видалення handler-а.
- Фінансові інваріанти мають бути однаковими для обох writer-ів; об’єднання реалізацій — тільки після characterization `HEALTH-05/HEALTH-06/HEALTH-07`.

**Що ще вирішити:** конкретний статус кожного legacy capability після inventory. Загальний принцип PWA-primary цього не замінює.

## DEC-03 — Канонічні контракти в Git, один живий backlog

**Статус: дозволено в межах поточного завдання; діє після merge docs-only PR.** Власник: Integrator.

- Публікуємо цей пакет у Git `docs/project-health/` (у звичайній зовнішній папці це `code/docs/project-health/`) через гілку → Conventional Commit → PR → required `validate` → review.
- [README](README.md) — одна стартова сторінка пакета; [реєстр](register.json) і HTML dashboard — датований audit snapshot, **не другий live task tracker**.
- Актуальний стан виконання після відкриття PR — у GitHub PR/Issues з `HEALTH-*` ID і посиланням на delivery receipt/evidence. Умови acceptance зберігати в перевірюваному Git-контракті.
- Інші зовнішні документи мігрувати тематично лише після звірки вимог. Цей PR не оголошує весь vault застарілим і не копіює recovery branch.
- Не змінюємо delivery CLI: на першому slice достатньо зв’язати backlog ID, requirement IDs, exact candidate, acceptance route та evidence через наявні receipts і PR.

**Merge/activation:** відкритий або green PR ще не означає, що новий entrypoint є на `main`. Docs-only пакет не потребує production redeploy.

## DEC-04 — Незалежне рев’ю без прихованої зміни protection

**Статус: незалежний огляд цього кандидата обов’язковий; варіант machine enforcement запропонований, не активований.** Власник: Release owner + Product owner.

- Зберігаємо required `validate`, admin enforcement, exact-SHA deployment і manual production approval. На readback 19 жовтня 2026 required approving reviews = **0**; число не змінюємо.
- Перед публікацією цього кандидата — незалежне read-only рев’ю початкових вимог і точного source candidate; verdict з прив’язкою до fingerprint зберігається у delivery evidence. Автор сам себе не approve-ить через GitHub.
- Рекомендований майбутній enforcement для solo-owner: перевірюваний candidate-bound review receipt/status check; якщо є реальний другий reviewer — required approval від нього. Реалізація потребує окремого погодженого `HEALTH-13`, threat model та failure-path тестів.
- Не видавати локальний review receipt за уже enforced GitHub protection. Не додавати фіктивного reviewer-а й не обходити захист admin merge.

**Що ще вирішити:** доступний незалежний reviewer чи окремий machine gate, і хто ним володіє. Поточний PR цього не впроваджує.

## Найближчий технічний slice

Після review/merge baseline — [HEALTH-05: normal transaction](HEALTH-05.md). Це characterization і захист поведінки до domain refactor. Partner/card policy, Telegram deprecation та protection enforcement не є його прихованими залежностями. Якщо необхідний browser/PostgreSQL/auth capability недоступний — статус **BLOCKED**, не synthetic PASS.
