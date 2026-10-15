from __future__ import annotations

from types import SimpleNamespace

from django.test import SimpleTestCase

from .services import build_ai_text_suggestion


class AiTextSuggestionTests(SimpleTestCase):
    def test_unrecognized_suggestion_preserves_clarification_message(self) -> None:
        suggestion = build_ai_text_suggestion(
            SimpleNamespace(base_currency="UAH"),
            {
                "is_candidate_tx": False,
                "intent": None,
                "clarification_message": "Введено кілька операцій — додайте їх окремо",
            },
        )

        self.assertEqual(
            suggestion,
            {
                "mode": "unrecognized",
                "message": "Введено кілька операцій — додайте їх окремо",
            },
        )
