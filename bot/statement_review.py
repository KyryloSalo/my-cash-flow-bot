"""Align review observations, not booking candidates, across independent passes."""
from difflib import SequenceMatcher
import re


def is_non_operation(row: dict) -> bool:
    if row.get('classification') == 'non_transaction':
        return True
    # Compatibility with older captures which had no explicit non-transaction enum.
    text = ' '.join(str(row.get(key) or '') for key in ('comment', 'review_reason')).casefold()
    explicit = re.search(r'\b(?:not (?:a |an )?(?:transaction|operation)(?: row)?|no (?:explicit )?transaction row)\b', text)
    header = re.search(r'\b(?:balance|header|cashback total)\b', text)
    signed = str(row.get('raw_amount') or '').lstrip().startswith(('+', '-', '−', '–'))
    return bool(explicit and header and not signed and not row.get('raw_status') and not row.get('raw_type'))


def _identity(row: dict, source: str, index: int) -> tuple:
    raw = row.get('raw_row') or {}
    raw_date = ' '.join(str(raw.get('raw_date') or '').casefold().split())
    tx_date = row.get('transaction_date')
    amount, currency = row.get('amount'), row.get('currency')
    if amount is None or not currency:
        # No evidence to assert that two incomplete observations are the same row.
        return ('unidentified', source, index)
    if not (tx_date or raw_date):
        literal_amount = ' '.join(str(raw.get('raw_amount') or '').split())
        if not literal_amount:
            return ('unidentified', source, index)
        # Without a printed date, only align the same physical row with matching
        # literal money. Never infer a date or collapse different row positions.
        return ('undated', row.get('row_index', index), amount, currency,
                row.get('direction'), literal_amount)
    return ('operation', str(tx_date) if tx_date else raw_date,
            amount, currency, row.get('direction'))


def _merge_observation(first: dict, second: dict) -> dict:
    merged = dict(first)
    reasons = list(dict.fromkeys([*first.get('review_reasons', [first.get('reason')]),
                                 *second.get('review_reasons', [second.get('reason')])]))
    merged['review_reasons'] = [reason for reason in reasons if reason]
    alternatives = list(first.get('alternative_raw_rows', []))
    raw = second.get('raw_row')
    if raw and raw != first.get('raw_row') and raw not in alternatives:
        alternatives.append(raw)
    if alternatives:
        merged['alternative_raw_rows'] = alternatives
    return merged


def align_review_rows(observations: list[tuple[str, list[dict]]]) -> tuple[dict, ...]:
    """Merge ordered pass sequences; retain within-pass multiplicity and unmatched rows."""
    rows: list[dict] = []
    keys: list[tuple] = []
    for source, incoming in observations:
        incoming_keys = [_identity(row, source, index) for index, row in enumerate(incoming)]
        combined_rows, combined_keys = [], []
        for tag, i, j, m, n in SequenceMatcher(None, keys, incoming_keys, autojunk=False).get_opcodes():
            if tag == 'equal':
                combined_rows.extend(_merge_observation(a, b) for a, b in zip(rows[i:j], incoming[m:n]))
                combined_keys.extend(keys[i:j])
            else:
                # Ambiguous/conflicting observations remain distinct and unconfirmed.
                combined_rows.extend(rows[i:j])
                combined_rows.extend(incoming[m:n])
                combined_keys.extend(keys[i:j])
                combined_keys.extend(incoming_keys[m:n])
        rows, keys = combined_rows, combined_keys
    return tuple(rows)
