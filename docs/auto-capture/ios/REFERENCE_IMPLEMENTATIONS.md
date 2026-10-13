# iOS capture reference implementations

Перевірено 2026-10-13. Це зовнішній feasibility evidence, а не Vydno production acceptance.

## Notification automation

### maximbilan/morph

- Repository: `https://github.com/maximbilan/morph`
- Initial notification handler: `https://github.com/maximbilan/morph/pull/79`
- Non-transaction filtering: `https://github.com/maximbilan/morph/pull/81`
- Incomplete payload diagnostics and bank/card mapping: `https://github.com/maximbilan/morph/pull/83`

Підтверджений патерн:

```text
bank notification
→ iOS 27 Shortcuts Notification automation
→ HTTPS POST with app/title/message/date
→ server-side classification
```

Це доводить технічну здійсненність transport path. Воно не гарантує locked-screen delivery, completeness, retry або однаковий payload для mono/Приват24 на іншому device/build.

Для Vydno беремо лише transport pattern. Не переносимо MoneyWiz/Telegram flow, AI-only classification, raw request logging або bank-specific mappings без власних sanitized fixtures.

## Wallet transaction automation

### MattFaz/actualtap

- Repository: `https://github.com/MattFaz/actualtap`
- Pattern: Wallet Transaction automation → HTTP API → expense system.

### kqtran-dev/iOS-transaction-shortcut

- Repository: `https://github.com/kqtran-dev/iOS-transaction-shortcut`
- Pattern: Wallet Transaction fields → `Run Script Over SSH`/external receiver.

Wallet references доводять automation для Apple Wallet/Apple Pay subset. Вони не читають довільні bank-app push і не покривають усі карткові операції.

## Apple constraints

- iOS 27 Event triggers: `https://support.apple.com/guide/shortcuts/apd932ff833f/ios`
- iOS 26 versioned guide does not document Notification trigger: `https://support.apple.com/guide/shortcuts/apd932ff833f/9.0/ios/26`
- Wallet Transaction trigger: `https://support.apple.com/guide/shortcuts/apd65c67538a/ios`
- HTTP API requests with Get Contents of URL: `https://support.apple.com/guide/shortcuts/apd58d46713f/ios`

Apple does not publish a stable bank-notification payload schema or durable webhook guarantees. Vydno must treat events as potentially delayed, missing, incomplete or duplicated.

## Vydno decision

Proceed with `ios_notification_automation` as a gated beta design against shared capture v1. Keep runtime submission disabled until all of the following are verified on a physical iPhone:

1. exact iOS version/build and bank app;
2. actual Shortcut Input fields and redacted normalized output;
3. unattended and locked-screen behavior;
4. privacy-preview behavior;
5. HTTPS delivery and duplicate behavior;
6. scoped credential storage without a secret in a public Shortcut.

Wallet remains a limited secondary source. Manual Share remains the fallback.