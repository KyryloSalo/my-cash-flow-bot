# iOS 27 Notification Shortcut blueprint

Статус: build-ready beta blueprint; не production-enabled без physical-device acceptance та shared capture-v1 foundation.

## User-created automation

PWA або imported Shortcut не може самостійно надати системні permissions чи створити Personal Automation за користувача. Користувач налаштовує:

1. Shortcuts → Automation → `+` → Notification.
2. Обирає лише підтримуваний bank app.
3. За можливості додає вузький filter за Title/Subtitle/Message.
4. Обирає `Run Immediately`, якщо цей режим доступний на перевіреному iOS build.
5. Викликає встановлений Vydno capture Shortcut із Notification input.

## Shortcut stages

1. Перевірити source app за локальним allowlist.
2. Витягти лише доступні input fields у пам'яті Shortcut.
3. Відкинути OTP/security/non-transaction notifications до network request.
4. Нормалізувати лише доведені `amount`, `currency`, `merchant`, `operation_hint`; невідомі значення лишити порожніми.
5. Створити stable event UUID до першої спроби відправлення.
6. Сформувати frozen capture-v1 payload із source `ios_notification_automation`.
7. Надіслати на canonical shared capture endpoint із scoped `capture:submit` credential.
8. Не викликати ledger/history/confirm endpoints.

## Payload boundary

Дозволений network payload після локальної sanitization:

```json
{
  "schema_version": 1,
  "event_id": "UUID",
  "source": "ios_notification_automation",
  "bank": "allowlisted-id-or-unknown",
  "observed_at": "RFC3339-with-timezone",
  "occurred_at": null,
  "amount": null,
  "currency": null,
  "merchant": null,
  "operation_hint": "unknown",
  "parser_version": "ios-notification/1"
}
```

Raw notification text, title, subtitle, PAN, OTP, balances, URLs, cookies and credentials не входять до capture-v1 payload і не записуються в logs/fixtures.

## Known blockers before installable artifact

- Shared foundation endpoint/schema fixture і pairing exchange ще мають надійти від foundation owner.
- Shortcuts не дає доведеної durable encrypted queue. Не обіцяти offline exactly-once.
- Безпечний credential path не визначений. Публічний Shortcut із вбудованим token заборонений; якщо Shortcuts storage недостатній, потрібен мінімальний App Intent + Keychain path і Apple signing.
- Physical-device test потрібен для actual fields, locked screen, preview modes і HTTPS completion.

До закриття цих пунктів reference adapter навмисно відхиляє `ios_notification_automation`.