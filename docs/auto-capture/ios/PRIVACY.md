# iOS privacy and security notes

- Adapter передає лише schema-v1 fields: event UUID, source, allowlisted bank ID, timestamps, decimal amount, currency, bounded merchant, operation hint і parser version.
- Raw push/share text, OTP, PAN, CVV/CVC/PIN, cookies, Safari/PWA session, Telegram ID, family/account ID та bank credentials заборонені.
- Невідомі або неповні дані залишаються `null`/`unknown`; UAH не підставляється автоматично.
- Automatic iOS sources вимкнені fail closed, доки physical gate не пройдено.
- Reference JSON queue призначена лише для contract tests із синтетичними даними. Вона не є production iOS storage і не доводить encrypted-at-rest або offline retry.
- Production credential має зберігатися через доведений Keychain/App Intent path або інший platform-secure mechanism. Не вбудовувати його в shared Shortcut.
- Повтор із тим самим local key має зберігати event UUID. Змінений фінансовий payload для того самого key конфліктує; adapter не створює новий UUID для обходу конфлікту.
- Network submit матиме лише `capture:submit`; ledger/history/confirm недоступні.
