# iOS auto-capture capability gate

Дата перевірки середовища: 2026-10-13. Base commit: `c545d83079573d4942c37931334d8ceddc17f8e8`.

## Рішення B0

**blocked device/tooling** для automatic capture. **manual only** доступний як чесний fallback і не називається автоматичним отриманням платежів.

На Windows-хості немає підключеного iPhone, Xcode/xcrun або libimobiledevice. Тому документація Apple не зарахована як device evidence. Джерела `ios_notification_automation` та `ios_wallet_automation` у reference adapter fail closed, доки таблицю нижче не заповнено результатами фізичного тесту.

## Capability matrix

| OS/build | Source | Payload fields | Automatic vs tap | Locked screen | Network | Credential storage | Limits |
|---|---|---|---|---|---|---|---|
| не перевірено | Notification Automation | не перевірено | не перевірено | не перевірено | не перевірено | не визначено | Потрібен фізичний iPhone; документація не доводить payload або автозапуск |
| не перевірено | Wallet Transaction | не перевірено | не перевірено | не перевірено | не перевірено | не визначено | Перевіряти лише якщо Notification недоступний/порожній; не вважати повною історією платежів |
| iOS із Share Sheet | Manual Share | лише поля, які користувач явно перевірив і передав | tap/manual | не застосовується | залежить від явного запуску | submit credential ще не інтегрований; shared foundation відсутній | Не automatic; немає доведеної durable iOS queue або exactly-once доставки |

## Фізичний чекліст

1. Записати модель iPhone та `Settings → General → About → iOS Version` разом із build.
2. У Shortcuts → Automation перевірити фактичну наявність Notification trigger, вибір mono/Приват24 і фільтри Title/Subtitle/Message.
3. Створити тимчасову локальну automation без secrets і без збереження raw text. Вивести лише булеву наявність полів та редаговані значення суми/валюти/merchant.
4. Перевірити окремо: app закритий; екран заблокований; previews Always/When Unlocked/Never; Ask Before Running; network request; затримка.
5. Не додавати реальний submit token у shared Shortcut, screenshot, URL, clipboard log або evidence.
6. Якщо Notification payload порожній — повторити мінімальний тест Wallet Transaction і зафіксувати, які типи оплат він реально охоплює.
7. Додати редаговане evidence до цієї таблиці. Не додавати повний текст пуша, PAN, OTP, credential або банківський логін.

## Умови зміни рішення

- `go notification`: доведені потрібні поля, автоматичний запуск, locked-screen behavior, network request і безпечний credential path.
- `go limited Wallet`: Notification не пройшов, але Wallet має чітко обмежений, доведений сценарій.
- `manual only`: автоматичні джерела не дають потрібних полів/запуску, але явний Share Sheet сценарій придатний.
- `blocked device/tooling`: немає фізичного evidence або signing/tooling для потрібного native path.
