# iOS pilot runbook

## Передумови

- B0 capability decision не `blocked device/tooling`.
- Shared capture-v1 foundation commit від Android/foundation owner доступний і прийнятий без дублювання migrations/backend.
- iOS adapter використовує погоджаний schema version 1 і scoped `capture:submit` credential.
- Тестувальник використовує власний opt-in account і погоджену малу покупку.

## Evidence

Записати без raw push/credentials: device, exact OS/build, Shortcut/native adapter version, parser/schema/release version, permission/privacy-preview state, source, timestamps, correlation/capture/draft/transaction IDs.

## Сценарії

1. Existing user pairing повертає того самого canonical user; ledger/history не дублюються.
2. App закритий і телефон заблокований: банк → одна capture item або явний `needs_review`; до confirm balance незмінний.
3. PWA review: вибір рахунку/категорії → explicit confirm → одна ledger operation.
4. Повтор доставки та два review confirms не створюють другу операцію.
5. Дві однакові реальні покупки не зливаються.
6. Transfer/refund/OTP/non-bank notification не стає витратою і не витікає в queue/network/logs.
7. Missing amount/currency лишається неповним; manual flow доступний.
8. Offline/restart/revoke/kill switch: фіксується лише реально доведена поведінка; confirm не чергується і не повторюється автоматично.
9. Safari та standalone повертаються до того самого identity.
10. Mixed-platform acceptance підтверджує незалежні flags і незмінний Android/shared contract.

## Stop conditions

Зупинити пілот при credential exposure, raw notification persistence, автоматичному ledger write, duplicate operation, невірній identity/scope або неможливості негайно revoke ingestion.
