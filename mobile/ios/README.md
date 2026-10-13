# Vydno iOS capture adapter

Цей каталог належить лише iOS-гілці. Він не містить Django backend, migrations, PWA inbox або financial confirm.

## Current capability

- Automatic Notification/Wallet capture: **disabled**, physical-device B0 не пройдено.
- Manual Share reference normalization: реалізовано для frozen capture schema v1.
- Pairing/network submission: очікує shared foundation contract; не емулюється другим backend.
- Production iOS queue/Keychain: не заявлено. `CaptureQueue` — локальний deterministic contract-test reference із синтетичними даними, не production storage.

## Tests

```bash
PYTHONPATH=mobile/ios python -m unittest discover -s mobile/ios/tests -v
```

Reference fixture: `fixtures/manual_share_event.v1.json`.

## Enablement rule

Не додавати `ios_notification_automation` або `ios_wallet_automation` до allowlist лише за документацією. Спочатку заповнити `docs/auto-capture/ios/CAPABILITY_MATRIX.md` реальним device evidence, потім обрати transport/storage design і перевірити його на фізичному пристрої.
