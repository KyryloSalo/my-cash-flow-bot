# Vydno iOS capture adapter

Цей каталог належить лише iOS-гілці. Він не містить Django backend, migrations, PWA inbox або financial confirm.

## Current capability

- Notification transport feasibility: **externally confirmed** на iOS 27 через `maximbilan/morph`; Vydno runtime submission лишається disabled до physical-device acceptance.
- Wallet transport feasibility: externally confirmed для обмеженого Apple Wallet/Apple Pay subset; не є повною банківською історією.
- Manual Share reference normalization: реалізовано для frozen capture schema v1.
- Pairing/network submission: очікує shared foundation contract; не емулюється другим backend.
- Production iOS queue/Keychain: не заявлено. `CaptureQueue` — локальний deterministic contract-test reference із синтетичними даними, не production storage.

## Tests

```bash
PYTHONPATH=mobile/ios python -m unittest discover -s mobile/ios/tests -v
```

Reference fixture: `fixtures/manual_share_event.v1.json`.

## Enablement rule

Не додавати `ios_notification_automation` або `ios_wallet_automation` до runtime allowlist лише за зовнішнім reference implementation. Blueprint можна розвивати, але enablement потребує `docs/auto-capture/ios/CAPABILITY_MATRIX.md` із Vydno device evidence, shared foundation contract і перевіреного credential path.

Notification setup/payload design: `docs/auto-capture/ios/NOTIFICATION_SHORTCUT_BLUEPRINT.md`.
