# Знахідки: ризик не дорівнює доведеному дефекту

[Dashboard](README.md) · [Реєстр](register.json) · [Baseline](baseline.json)

Основа: snapshot `f2e53b659cee68da7d1cfb6ebabd89384fe180e2`, два незалежні read-only аудити та cross-check інтегратора. Повного product test suite й authenticated acceptance тут не запускали.

## F01. Два фінансові writer-и: Django та asyncpg

**Клас:** `confirmed-risk`. **Впевненість:** `high`.

Однакові інваріанти мають окремі реалізації; конкретна розбіжність DB-результату цим аудитом не доведена.

**Evidence anchors:** `bot/transaction_service.py:83`

## F02. Browser smoke не є фінансовим end-to-end

**Клас:** `confirmed-gap`. **Впевненість:** `high`.

Наявний маршрут навмисно не перевіряє draft/confirm/payment/Telegram/provider. Не називати його feature acceptance.

**Evidence anchors:** `docs/BROWSER_ACCEPTANCE.md:16`

## F03. Receipt не пов’язаний наскрізно з вимогою й релізом

**Клас:** `confirmed-gap`. **Впевненість:** `high`.

Локальний candidate-bound delivery уже сильний; потрібні посилання на backlog/scenario/PR/release, а не другий engine.

**Evidence anchors:** `ops/delivery.py:190`

## F04. Джерела вимог і current state розділені між outer workspace і Git

**Клас:** `confirmed-drift`. **Впевненість:** `high`.

Main містить runtime і release metadata; частина чинних людських вимог/стану лишається зовні. Міграція лише після звірки.

**Evidence anchors:** `docs/releases/0.40.3-canonical-docs.json:1`

## F05. Review потрібен правилами, але branch protection дозволяє 0 approvals

**Клас:** `confirmed-configuration`. **Впевненість:** `high`.

Required validate та admin enforcement є; machine enforcement незалежного review потребує узгодженого рішення.

**Evidence anchors:** `baseline.json:17`

## F06. Onboarding використовує host date, інші гілки — application date

**Клас:** `unreproduced-risk`. **Впевненість:** `medium`.

Різні API підтверджені статично. Невірну дату у runtime/production тут не відтворено; спочатку timezone-boundary RED.

**Evidence anchors:** `admin_service/miniapp/onboarding.py:179`

## F07. Великі orchestration-модулі збільшують blast radius

**Клас:** `confirmed-complexity`. **Впевненість:** `high`.

Розмір — сигнал концентрації, не доказ дефекту. Першими змінювати seams після characterization, не wholesale rewrite.

**Evidence anchors:** `bot/bot_main.py:1`

## F08. Partner trial/card очікування не можна виводити лише з одного документа

**Клас:** `confirmed-policy-conflict`. **Впевненість:** `high`.

Canonical main partner_offers.py:50–51 резервує claim, доступ після card binding. Натомість recovery checkout code/docs/PARTNER_TRIAL_OFFERS.md:14–15 обіцяє grant без картки/renewal. Це конфлікт різних revision/source layers, не доказ суперечності всередині одного deployed SHA. Спершу рішення власника.

**Evidence anchors:** `admin_service/miniapp/partner_offers.py:15`

## F09. Докази acceptance можуть пережити строк зберігання CI artifacts

**Клас:** `confirmed-gap`. **Впевненість:** `high`.

Browser logs 14 днів, bundle 30 днів у перевіреному workflow; summary/hash/status прив’язувати до вимоги.

**Evidence anchors:** `.github/workflows/deploy.yml:81`

## F10. Family та photo/text/voice потребують наскрізних sibling-сценаріїв

**Клас:** `coverage-risk`. **Впевненість:** `high`.

Частина suite використовує patched commit і synthetic ledger. Це корисний orchestration guard, але не DB/provider acceptance.

**Evidence anchors:** `admin_service/miniapp/tests_mini_confirmation_integrity.py:47`

## Що зберегти

Наявні candidate-bound `delivery.py`, required `validate`, isolated browser smoke, exact-image artifact та окремий production workflow з manual approval/readback. Вони вже розв’язують важливу частину задачі. Новий паралельний engine не потрібен.

## Межі

Не відтворено конкретну runtime-регресію financial commit, timezone boundary чи family authorization. Не виконано real PSP bind, Telegram identity login або physical capture. Не підтверджено повний runtime config/workers graph. Не читали credential-файли й не виконували production writes. HTTP 200 і deployment artifact не підміняють ці перевірки.

Source-layer conflict F08 має два точні джерела: canonical `admin_service/miniapp/partner_offers.py:50–51` та recovery checkout `code/docs/PARTNER_TRIAL_OFFERS.md:14–15` у зовнішній робочій копії. Другий не є файлом canonical audit SHA. Саме тому переносити recovery-код wholesale небезпечно.
