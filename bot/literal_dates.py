"""Complete printed dates only; never supply a missing year or repair a date."""
from datetime import date, datetime
import re


def parse_literal_date(value: object) -> date | None:
    text = value.strip() if isinstance(value, str) else ""
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    if re.fullmatch(
        r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?", text
    ):
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            return None
    match = re.fullmatch(
        r"(?P<day>\d{1,2})(?:\.(?P<month>\d{1,2})\.|\s+(?P<month_name>[а-яіїєґ]+)\s+)"
        r"(?P<year>\d{4})(?:,?\s+(?P<hour>\d{2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?)?",
        text.lower(),
    )
    if match is None:
        return None
    months = ("січня", "лютого", "березня", "квітня", "травня", "червня",
              "липня", "серпня", "вересня", "жовтня", "листопада", "грудня")
    try:
        month = int(match["month"]) if match["month"] else months.index(match["month_name"]) + 1
        return datetime(int(match["year"]), month, int(match["day"]),
                        int(match["hour"] or 0), int(match["minute"] or 0),
                        int(match["second"] or 0)).date()
    except ValueError:
        return None


def verified_literal_date(claim: object, literal: object) -> date | None:
    observed = parse_literal_date(literal)
    if observed is None:
        return None
    if claim is None or claim == "":
        return observed
    try:
        candidate = date.fromisoformat(claim) if isinstance(claim, str) else None
    except ValueError:
        return None
    return observed if candidate == observed else None
