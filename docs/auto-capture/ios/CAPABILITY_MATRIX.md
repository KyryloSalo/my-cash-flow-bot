# iOS auto-capture capability gate

Дата перевірки середовища: 2026-10-13. Base commit: `c545d83079573d4942c37931334d8ceddc17f8e8`.

## Рішення B0

**External feasibility confirmed; Vydno device acceptance blocked.** Незалежна реалізація `maximbilan/morph` підтверджує робочий транспортний патерн iOS 27 `Notification automation → HTTPS webhook`, зокрема для реальних банківських push. Це дозволяє готувати Vydno Shortcut/adapter як beta, але не є доказом підтримки конкретного iPhone, банку або privacy configuration.

На Windows-хості немає підключеного iPhone, Xcode/xcrun або libimobiledevice. Тому джерела `ios_notification_automation` та `ios_wallet_automation` у reference adapter залишаються fail closed до Vydno physical-device acceptance. **Manual Share** доступний як чесний fallback і не називається автоматичним отриманням платежів.

## Capability matrix

| OS/build | Source | Payload fields | Automatic vs tap | Locked screen | Network | Credential storage | Limits |
|---|---|---|---|---|---|---|---|
| iOS 27, external reference only | Notification Automation | external implementation передає app/title/message/date; Vydno fields не перевірено | transport реалізований зовнішнім проєктом; Vydno не перевірено | не перевірено для Vydno | HTTPS POST реалізований зовнішнім проєктом; retry/delivery guarantee відсутня | Vydno credential path не визначено | Готувати як beta; потрібен Vydno acceptance для конкретних device/bank/privacy settings |
| iOS 17.2+, external references only | Wallet Transaction | community implementations використовують card/merchant/amount/date; Vydno fields не перевірено | `Run Immediately` описано у reference implementations | поведінка залежить від trigger/action і потребує Vydno test | webhook/SSH patterns існують; можливі timeout/delay | Vydno credential path не визначено | Лише Wallet/Apple Pay subset, не повна банківська історія |
| iOS із Share Sheet | Manual Share | лише поля, які користувач явно перевірив і передав | tap/manual | не застосовується | залежить від явного запуску | submit credential ще не інтегрований; shared foundation відсутній | Не automatic; немає доведеної durable iOS queue або exactly-once доставки |

## Фізичний чекліст

1. Записати модель iPhone та `Settings → General → About → iOS Version` разом із build.
2. У Shortcuts → Automation перевірити фактичну наявність Notification trigger, вибір mono/Приват24 і фільтри Title/Subtitle/Message.
3. Створити тимчасову локальну automation без secrets і без збереження raw text. Вивести лише булеву наявність полів та редаговані значення суми/валюти/merchant.
4. Перевірити окремо: app закритий; екран заблокований; previews Always/When Unlocked/Never; Ask Before Running; network request; затримка.
5. Не додавати реальний submit token у shared Shortcut, screenshot, URL, clipboard log або evidence.
6. Якщо Notification payload порожній — повторити мінімальний тест Wallet Transaction і зафіксувати, які типи оплат він реально охоплює.
7. Додати редаговане evidence до цієї таблиці. Не додавати повний текст пуша, PAN, OTP, credential або банківський логін.

## External references

- `maximbilan/morph`: iOS 27 bank notification → Shortcut → `POST /notificationHandler`; див. PRs `#79`, `#81`, `#83`.
- `MattFaz/actualtap`: Wallet Transaction automation → webhook/API.
- `kqtran-dev/iOS-transaction-shortcut`: Wallet Transaction fields → external server.
- Apple iOS 27 Event triggers: Notification trigger з App та Message/Subtitle/Title filters.

Деталі та межі повторного використання: `REFERENCE_IMPLEMENTATIONS.md`.

## Умови зміни рішення

- `go notification`: доведені потрібні поля, автоматичний запуск, locked-screen behavior, network request і безпечний credential path.
- `go limited Wallet`: Notification не пройшов, але Wallet має чітко обмежений, доведений сценарій.
- `manual only`: автоматичні джерела не дають потрібних полів/запуску, але явний Share Sheet сценарій придатний.
- `external feasibility confirmed; Vydno device acceptance blocked`: транспортний патерн доведений зовнішньо, але немає Vydno physical-device evidence.
- `blocked device/tooling`: немає фізичного evidence або signing/tooling для потрібного native path і відсутній придатний зовнішній доказ transport feasibility.
