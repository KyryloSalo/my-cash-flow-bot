from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re
from typing import Any

from normalization_compiler import normalize_text


_SPACE_RE = re.compile(r"\s+")
_COMPATIBILITY_PHRASES = (
    {
        "matched_slug": "taxi",
        "matched_entity": "compatibility",
        "aliases": ["bolt taxi", "bitaksi", "uklon", "taxi", "uber", "bolt", "\u0442\u0430\u043a\u0441\u0456", "\u0442\u0430\u043a\u0441\u0438"],
        "explanation": "compatibility taxi keyword",
    },
    {
        "matched_slug": "cafes_restaurants_delivery",
        "matched_entity": "compatibility",
        "aliases": ["coffee", "cafe", "restaurant", "restoran", "\u043a\u0430\u0432\u0430", "\u043a\u043e\u0444\u0435"],
        "explanation": "compatibility cafes keyword",
    },
    {
        "matched_slug": "subscriptions_services",
        "matched_entity": "compatibility",
        "aliases": ["apple com bill", "apple bill", "apple", "google", "netflix", "spotify", "youtube", "chatgpt", "openai"],
        "explanation": "compatibility subscription keyword",
    },
    {
        "matched_slug": "health",
        "matched_entity": "compatibility",
        "aliases": ["eczanesi", "eczane", "apteka", "pharmacy"],
        "explanation": "compatibility health keyword",
    },
)


@dataclass(frozen=True)
class NormalizationMatch:
    matched_slug: str
    confidence: float
    matched_entity: str
    matched_alias: str
    rule_id: str | None
    subcategory: str | None
    explanation: str


def _confidence(stage_base: float, priority: Any) -> float:
    try:
        score = int(priority)
    except (TypeError, ValueError):
        score = 0
    return round(min(0.99, stage_base + min(max(score, 0), 100) / 1000.0), 3)


def _contains_phrase(text: str, phrase: str) -> bool:
    if not text or not phrase:
        return False
    padded = f" {text} "
    return f" {phrase} " in padded


def _strip_stopwords(text: str, stopwords: list[str]) -> str:
    if not text:
        return ""
    padded = f" {text} "
    for stopword in stopwords:
        phrase = f" {stopword} "
        if phrase in padded:
            padded = padded.replace(phrase, " ")
    return _SPACE_RE.sub(" ", padded).strip()


class NormalizationService:
    def __init__(self, runtime: dict[str, Any] | None = None) -> None:
        runtime = runtime or {}
        self._stopwords = [str(item) for item in runtime.get("stopwords", []) if str(item).strip()]
        self._exact_entries = [dict(item) for item in runtime.get("exact_aliases", [])]
        self._product_entries = [dict(item) for item in runtime.get("product_aliases", [])]
        self._category_entries = [dict(item) for item in runtime.get("category_keywords", [])]
        self._regex_rules = [dict(item) for item in runtime.get("regex_rules", [])]
        self._carrier_aliases = [str(item) for item in runtime.get("routing", {}).get("carrier_aliases", []) if str(item).strip()]
        self._food_terms = [str(item) for item in runtime.get("routing", {}).get("food_terms", []) if str(item).strip()]

        self._non_carrier_exact_entries = [entry for entry in self._exact_entries if not entry.get("routing_carrier")]
        self._health_entries = [entry for entry in self._non_carrier_exact_entries if entry.get("matched_slug") == "health"]
        self._grocery_entries = [entry for entry in self._non_carrier_exact_entries if entry.get("matched_slug") == "groceries"]
        self._restaurant_entries = [
            entry for entry in self._non_carrier_exact_entries if entry.get("matched_slug") == "cafes_restaurants_delivery"
        ]
        self._health_category_entries = [entry for entry in self._category_entries if entry.get("matched_slug") == "health"]
        self._grocery_category_entries = [entry for entry in self._category_entries if entry.get("matched_slug") == "groceries"]

        self._compiled_regex_rules: list[tuple[re.Pattern[str], dict[str, Any]]] = []
        for rule in self._regex_rules:
            pattern = str(rule.get("pattern") or "").strip()
            if not pattern:
                continue
            try:
                self._compiled_regex_rules.append((re.compile(pattern), rule))
            except re.error:
                continue
        self._compatibility_entries: list[dict[str, Any]] = []
        for block in _COMPATIBILITY_PHRASES:
            for alias in block["aliases"]:
                self._compatibility_entries.append(
                    {
                        "alias": alias,
                        "matched_slug": block["matched_slug"],
                        "matched_entity": block["matched_entity"],
                        "priority": 99,
                        "subcategory": None,
                        "routing_carrier": False,
                        "explanation": block["explanation"],
                    }
                )
        self._compatibility_health_entries = [
            entry for entry in self._compatibility_entries if entry["matched_slug"] == "health"
        ]

    @classmethod
    def load_from_path(cls, path: Path) -> "NormalizationService":
        try:
            runtime = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            runtime = {}
        return cls(runtime)

    def match_expense(self, text: str) -> NormalizationMatch | None:
        normalized = normalize_text(text)
        if not normalized:
            return None
        cleaned = _strip_stopwords(normalized, self._stopwords)

        exact_match = self._match_alias_entries(cleaned, self._non_carrier_exact_entries, stage_base=0.9)
        if exact_match is not None:
            return exact_match

        routing_match = self._match_routing(cleaned)
        if routing_match is not None:
            return routing_match

        compatibility_match = self._match_compatibility(cleaned)
        if compatibility_match is not None:
            return compatibility_match

        product_match = self._match_alias_entries(cleaned, self._product_entries, stage_base=0.86)
        if product_match is not None:
            return product_match

        category_match = self._match_alias_entries(cleaned, self._category_entries, stage_base=0.82)
        if category_match is not None:
            return category_match

        regex_match = self._match_regex(normalized)
        if regex_match is not None:
            return regex_match

        return None

    def _match_alias_entries(
        self,
        text: str,
        entries: list[dict[str, Any]],
        *,
        stage_base: float,
    ) -> NormalizationMatch | None:
        for entry in entries:
            alias = str(entry.get("alias") or "").strip()
            if alias and _contains_phrase(text, alias):
                return NormalizationMatch(
                    matched_slug=str(entry.get("matched_slug") or ""),
                    confidence=_confidence(stage_base, entry.get("priority")),
                    matched_entity=str(entry.get("matched_entity") or "category"),
                    matched_alias=alias,
                    rule_id=None,
                    subcategory=str(entry.get("subcategory") or "").strip() or None,
                    explanation=f"exact {entry.get('matched_entity') or 'category'} alias: {alias}",
                )
        return None

    def _match_routing(self, text: str) -> NormalizationMatch | None:
        carrier_alias = self._find_phrase(text, self._carrier_aliases)
        if not carrier_alias:
            return None

        merchant_match = self._match_alias_entries(text, self._health_entries, stage_base=0.94)
        if merchant_match is not None:
            return NormalizationMatch(
                matched_slug=merchant_match.matched_slug,
                confidence=0.97,
                matched_entity="routing",
                matched_alias=f"{carrier_alias} + {merchant_match.matched_alias}",
                rule_id="routing:carrier_health_merchant",
                subcategory=merchant_match.subcategory,
                explanation="delivery carrier plus health merchant context",
            )

        product_match = self._match_alias_entries(text, self._product_entries, stage_base=0.86)
        if product_match is not None:
            return NormalizationMatch(
                matched_slug="groceries",
                confidence=0.97,
                matched_entity="routing",
                matched_alias=f"{carrier_alias} + {product_match.matched_alias}",
                rule_id="routing:carrier_product",
                subcategory=product_match.subcategory,
                explanation="delivery carrier plus grocery product context",
            )

        grocery_match = self._match_alias_entries(text, self._grocery_entries, stage_base=0.94)
        if grocery_match is not None:
            return NormalizationMatch(
                matched_slug="groceries",
                confidence=0.97,
                matched_entity="routing",
                matched_alias=f"{carrier_alias} + {grocery_match.matched_alias}",
                rule_id="routing:carrier_grocery_merchant",
                subcategory=grocery_match.subcategory,
                explanation="delivery carrier plus grocery merchant context",
            )

        health_keyword = self._match_alias_entries(text, self._health_category_entries, stage_base=0.82)
        if health_keyword is not None:
            return NormalizationMatch(
                matched_slug="health",
                confidence=0.965,
                matched_entity="routing",
                matched_alias=f"{carrier_alias} + {health_keyword.matched_alias}",
                rule_id="routing:carrier_health_keyword",
                subcategory=health_keyword.subcategory,
                explanation="delivery carrier plus health keyword context",
            )

        compatibility_health = self._match_alias_entries(text, self._compatibility_health_entries, stage_base=0.89)
        if compatibility_health is not None:
            return NormalizationMatch(
                matched_slug="health",
                confidence=0.965,
                matched_entity="routing",
                matched_alias=f"{carrier_alias} + {compatibility_health.matched_alias}",
                rule_id="routing:carrier_health_compatibility",
                subcategory=None,
                explanation="delivery carrier plus compatibility health keyword",
            )

        food_term = self._find_phrase(text, self._food_terms)
        if food_term:
            return NormalizationMatch(
                matched_slug="cafes_restaurants_delivery",
                confidence=0.965,
                matched_entity="routing",
                matched_alias=f"{carrier_alias} + {food_term}",
                rule_id="routing:carrier_food_term",
                subcategory=None,
                explanation="delivery carrier plus food term context",
            )

        grocery_keyword = self._match_alias_entries(text, self._grocery_category_entries, stage_base=0.82)
        if grocery_keyword is not None:
            return NormalizationMatch(
                matched_slug="groceries",
                confidence=0.955,
                matched_entity="routing",
                matched_alias=f"{carrier_alias} + {grocery_keyword.matched_alias}",
                rule_id="routing:carrier_grocery_keyword",
                subcategory=grocery_keyword.subcategory,
                explanation="delivery carrier plus grocery keyword context",
            )

        restaurant_match = self._match_alias_entries(text, self._restaurant_entries, stage_base=0.94)
        if restaurant_match is not None:
            return NormalizationMatch(
                matched_slug="cafes_restaurants_delivery",
                confidence=0.965,
                matched_entity="routing",
                matched_alias=f"{carrier_alias} + {restaurant_match.matched_alias}",
                rule_id="routing:carrier_restaurant_merchant",
                subcategory=restaurant_match.subcategory,
                explanation="delivery carrier plus restaurant merchant context",
            )

        return NormalizationMatch(
            matched_slug="cafes_restaurants_delivery",
            confidence=0.94,
            matched_entity="routing",
            matched_alias=carrier_alias,
            rule_id="routing:carrier_default",
            subcategory=None,
            explanation="delivery carrier without stronger grocery or health context",
        )

    def _match_regex(self, text: str) -> NormalizationMatch | None:
        for compiled, rule in self._compiled_regex_rules:
            if compiled.search(text):
                pattern = str(rule.get("pattern") or "")
                return NormalizationMatch(
                    matched_slug=str(rule.get("matched_slug") or ""),
                    confidence=_confidence(0.8, rule.get("priority")),
                    matched_entity=str(rule.get("matched_entity") or "category_regex"),
                    matched_alias=pattern,
                    rule_id=f"regex:{pattern}",
                    subcategory=str(rule.get("subcategory") or "").strip() or None,
                    explanation=f"regex rule match: {pattern}",
                )
        return None

    def _match_compatibility(self, text: str) -> NormalizationMatch | None:
        for entry in self._compatibility_entries:
            alias = str(entry.get("alias") or "")
            if alias and _contains_phrase(text, alias):
                return NormalizationMatch(
                    matched_slug=str(entry.get("matched_slug") or ""),
                    confidence=0.935,
                    matched_entity=str(entry.get("matched_entity") or "compatibility"),
                    matched_alias=alias,
                    rule_id=f"compatibility:{alias}",
                    subcategory=None,
                    explanation=str(entry.get("explanation") or "compatibility keyword"),
                )
        return None

    @staticmethod
    def _find_phrase(text: str, phrases: list[str]) -> str | None:
        for phrase in phrases:
            if phrase and _contains_phrase(text, phrase):
                return phrase
        return None


@lru_cache(maxsize=1)
def get_normalization_service() -> NormalizationService:
    runtime_path = Path(__file__).with_name("normalization_runtime.json")
    return NormalizationService.load_from_path(runtime_path)


def match_expense_normalization(text: str) -> NormalizationMatch | None:
    return get_normalization_service().match_expense(text)
