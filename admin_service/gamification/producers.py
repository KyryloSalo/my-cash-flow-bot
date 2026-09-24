from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from .models import GamificationEventOutbox


_SOURCE_INPUT_METHODS = {
    "manual": "manual",
    "miniapp_manual": "manual",
    "miniapp_billing_expense": "manual",
    "text": "text",
    "ai_text": "text",
    "voice": "voice",
    "ai_voice": "voice",
    "screenshot": "screenshot",
    "ai_screenshot": "screenshot",
    "image": "screenshot",
    "photo": "screenshot",
    "import": "import",
}


def normalize_input_method(source: str | None) -> str:
    normalized = str(source or "").strip().lower()
    return _SOURCE_INPUT_METHODS.get(normalized, "")


def enqueue_transaction_created(
    *,
    actor_user_id: int,
    space_id: int | None,
    transaction_id: int,
    accepted_at: datetime,
    source: str,
    transaction_type: str,
    amount: Decimal,
    flow_kind: str,
) -> GamificationEventOutbox:
    event_id = uuid5(NAMESPACE_URL, f"vydno:transaction.created:{int(transaction_id)}")
    event, _created = GamificationEventOutbox.objects.get_or_create(
        event_id=event_id,
        defaults={
            "event_type": "transaction.created",
            "actor_user_id": int(actor_user_id),
            "space_id": int(space_id) if space_id is not None else None,
            "entity_type": "transaction",
            "entity_id": str(int(transaction_id)),
            "accepted_at": accepted_at,
            "input_method": normalize_input_method(source),
            "payload": {
                "transaction_type": str(transaction_type),
                "amount": f"{Decimal(str(amount)):.2f}",
                "flow_kind": str(flow_kind),
            },
        },
    )
    return event
