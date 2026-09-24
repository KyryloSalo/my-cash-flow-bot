# Ресурси ачівок Vydno

Це канонічна папка готових візуалів та анімацій Mini App. Комплект із
`C:\Users\User\Desktop\My Cash Flow Bot\Vydno_Achievements_Kit\vydno-achievements-kit`
уже імпортовано сюди без перейменування файлів і перевірено за manifest.

```text
achievements/
├─ assets/
│  ├─ bob/       # кольорові PNG, *-locked.png і GIF Боба
│  └─ capi/      # кольорові PNG, *-locked.png і GIF Капі
├─ animation/
│  ├─ achievements.css   # основний production motion layer
│  └─ README.md
└─ previews/
   ├─ mascot-selection-concept.png
   └─ оглядові PNG/GIF
```

## Перевірений комплект

Для **кожного** талісмана:

- 36 кольорових PNG;
- 36 PNG-силуетів із суфіксом `-locked.png`;
- 36 GIF-анімацій;
- PNG: `512 × 512 px`;
- GIF: `256 × 256 px`, один цикл близько 3 секунд.

Разом у `assets/`: 216 файлів. Усі 216 пройшли перевірку розміру та SHA-256
за `docs/achievements-kit/assets-manifest.json`.

## Важливо

- Не перейменовуйте сцени та не змінюйте регістр у назвах.
- GIF кладіть поруч із PNG відповідного талісмана: `assets/bob/` або `assets/capi/`.
- Не кладіть сюди `catalog.json` із серверними умовами секретних ачівок: ця директорія публічна через Django static files. Source snapshot зберігається у `docs/achievements-kit/`.
- Не оптимізуйте й не конвертуйте оригінали перед завантаженням. Після завантаження окремо перевіримо назви, кількість, розміри та сформуємо production-версії.
- Файли `.gitkeep` можна залишити — вони лише утримують порожню структуру папок.
- У runtime використовувати PNG + CSS. GIF зберігати як reference/точковий fallback, не передзавантажувати всі 72 й не анімувати весь каталог одночасно.
- PNG мають непрозорий світлий фон; для locked-стану використовувати готовий `-locked.png`, а не CSS-фільтр.
