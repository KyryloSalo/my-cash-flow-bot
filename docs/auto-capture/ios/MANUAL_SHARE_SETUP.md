# Ручне передавання з iPhone до Vydno

Це fallback, а не automatic capture. Він не читає чужі push-сповіщення у фоні й не підтверджує фінансову операцію.

## Поточний стан

Reference adapter у `mobile/ios/capture_adapter.py` формує лише frozen capture-v1 payload із `source=ios_manual_share`. Pairing, submit endpoint і PWA inbox мають прийти зі shared foundation; у цій гілці вони не дублюються.

## UX для майбутнього Shortcut

1. Користувач відкриває Share Sheet і обирає Vydno Shortcut.
2. Shortcut просить перевірити суму, валюту, merchant та тип операції. Відсутні дані лишаються порожніми; валюта не вгадується.
3. До network формується тільки нормалізований payload. Raw notification/screenshot/text не надсилається і не журналюється.
4. Подія потрапляє у спільний persistent capture inbox зі статусом `needs_review` за неповних/неоднозначних даних.
5. У PWA користувач окремо вибирає рахунок/категорію та підтверджує чинним financial confirm. Shortcut не має ledger/history/confirm доступу.

## Що не можна обіцяти

- фонове автоматичне отримання платежів;
- повну банківську виписку;
- offline exactly-once, поки немає доведеної durable iOS queue;
- автоматичне створення Automation або надання дозволів із PWA;
- підтримку конкретного банку чи iOS version без physical-device evidence.

## Credential boundary

Публічний Shortcut не містить приватного submit token. До появи shared pairing flow network submission вимкнений. Якщо Shortcuts не дозволить приховати credential, потрібен мінімальний native App Intent + Keychain шлях і реальний signing/device gate; секрет у Shortcut dictionary, URL, clipboard або iCloud template неприйнятний.
