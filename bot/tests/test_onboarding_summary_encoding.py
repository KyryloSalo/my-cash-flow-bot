from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import bot_main


class OnboardingSummaryEncodingTests(IsolatedAsyncioTestCase):
    async def test_summary_is_readable_ukrainian_without_mojibake(self):
        with (
            patch.object(
                bot_main,
                "_get_user",
                new=AsyncMock(
                    return_value={
                        "lang": "uk",
                        "base_currency": "UAH",
                        "start_date": __import__("datetime").date(2026, 9, 15),
                    }
                ),
            ),
            patch.object(bot_main, "_render_accounts_text", new=AsyncMock(return_value="Основний · 1000 UAH")),
        ):
            text = await bot_main._render_onboarding_summary(object(), 1, {})

        self.assertIn("Крок 5 з 5: підтвердження.", text)
        self.assertIn("Базова валюта: UAH", text)
        self.assertIn("Основний · 1000 UAH", text)
        self.assertNotIn("Рљ", text)
        self.assertNotIn("СЂ", text)


if __name__ == "__main__":
    __import__("unittest").main()
