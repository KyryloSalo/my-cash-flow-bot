"""Shadow-only, fail-closed photo evidence pipeline."""
from __future__ import annotations
import hashlib, re
from io import BytesIO
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any
_CURRENCY={"UAH":{"UAH","ГРН","₴"},"EUR":{"EUR","€","EURO"},"USD":{"USD","US$"},"TRY":{"TRY","TL","₺","LIRA","LIRAS"}}
_STATUS={"completed":{"completed","paid","successful","успішно","оплачено","başarılı"},"declined":{"declined","rejected","failed","відхилено","отклонен","başarısız"},"refunded":{"refunded","refund","reversed","повернення","iade"},"pending":{"pending","processing","очікує","очікування","в обробці","beklemede"}}
_TYPE={"expense":{"expense","purchase","payment","витрата","оплата","покупка","harcama"},"income":{"income","credit","дохід","зарахування","gelir"},"transfer":{"transfer","переказ","перевод","havale"},"own_account_transfer":{"own account","власний рахунок","свій рахунок","kendi hesabı"},"refund":{"refund","refunded","повернення","iade"}}
@dataclass(frozen=True)
class Evidence: id:str; field:str; raw_text:str; value:str; region:str|None=None
@dataclass(frozen=True)
class ExtractionPass:
    source: str
    evidence: tuple[Evidence,...] = ()
    line_items: tuple[dict[str,Any],...] = ()
    receipt_total: Decimal|None = None
    transaction_date: Any = None
    merchant: str|None = None
    raw_evidence: dict[str,Any]|None = None
    mode: str = "single_tx"
@dataclass(frozen=True)
class PipelineConfig: preprocessing_version:str="photo-preprocess-v1"; prompt_version:str="photo-evidence-v5"; schema_version:str="photo-schema-v2"

@dataclass(frozen=True)
class ImageQuality:
    width: int; height: int; blur_score: float; contrast: float; clipped_edges: bool; glare_ratio: float

def assess_image_quality(image_bytes: bytes) -> ImageQuality:
    """Return non-secret, deterministic quality signals for trace/review."""
    try:
        from PIL import Image, ImageStat
        with Image.open(BytesIO(image_bytes)) as image:
            gray = image.convert("L")
            stat = ImageStat.Stat(gray)
            pixels = list(gray.resize((min(256, gray.width), min(256, gray.height))).getdata())
            clipped = sum(p <= 2 or p >= 253 for p in pixels) / max(1, len(pixels))
            # Variance is a conservative blur proxy; it is only a signal, not a gate.
            variance = float(stat.var[0])
            glare = sum(p >= 250 for p in pixels) / max(1, len(pixels))
            return ImageQuality(image.width, image.height, round(variance, 3), round(stat.stddev[0], 3), clipped > .08, round(glare, 4))
    except Exception as exc:
        raise ValueError("Malformed or unsupported image") from exc
@dataclass(frozen=True)
class CacheKey:
    original_sha256:str; preprocessing_version:str; model:str; detail:str; prompt_version:str; schema_version:str
    def as_string(self)->str:return ":".join((self.original_sha256,self.preprocessing_version,self.model,self.detail,self.prompt_version,self.schema_version))
@dataclass(frozen=True)
class ReconciliationResult:
    accepted:bool; requires_review:bool; amount:Decimal|None; currency:str|None; status:str|None; transaction_type:str|None; evidence_ids:tuple[str,...]; findings:tuple[str,...]; trace:dict[str,Any]; transaction_date:Any=None; merchant:str|None=None; receipt_level:str|None=None; line_items:tuple[dict[str,Any],...]=(); receipt_total:Decimal|None=None; raw_evidence:tuple[dict[str,Any],...]=(); statement_items:tuple[dict[str,Any],...]=(); statement_review_rows:tuple[dict[str,Any],...]=(); skipped_items_count:int=0
def plan_vertical_tiles(*,height:int,tile_height:int,overlap:float=.2)->tuple[tuple[int,int],...]:
    if height<=0:raise ValueError("height must be positive")
    if tile_height<=0:raise ValueError("tile_height must be positive")
    if not 0<=overlap<1:raise ValueError("overlap must be in [0, 1)")
    if tile_height>=height:return ((0,height),)
    step=max(1,int(tile_height*(1-overlap))); starts=list(range(0,height-tile_height+1,step)); final=height-tile_height
    if starts[-1]!=final:starts.append(final)
    return tuple((s,min(s+tile_height,height)) for s in starts)
def build_cache_key(image_bytes:bytes,*,config:PipelineConfig,model:str,detail:str)->CacheKey:return CacheKey(hashlib.sha256(image_bytes).hexdigest(),config.preprocessing_version,model,detail,config.prompt_version,config.schema_version)
def parse_raw_amount(raw:str)->Decimal|None:
    """Parse unambiguous two-decimal money without correcting OCR digits.

    A lone separator followed by three digits is ambiguous without locale evidence.
    Space grouping must consist of three-digit groups; malformed grouping is rejected.
    """
    text = str(raw or "").strip().replace("\u00a0", " ").replace("\u202f", " ")
    if not text or len(text) > 40:
        return None
    plain = r"[0-9]+(?:[.,][0-9]{1,2})?"
    spaced = r"[0-9]{1,3}(?: [0-9]{3})+(?:[.,][0-9]{1,2})?"
    comma_grouped = r"[0-9]{1,3}(?:,[0-9]{3})+\.[0-9]{1,2}"
    dot_grouped = r"[0-9]{1,3}(?:\.[0-9]{3})+,[0-9]{1,2}"
    if re.fullmatch(plain, text) or re.fullmatch(spaced, text):
        normalized = text.replace(" ", "").replace(",", ".")
    elif re.fullmatch(comma_grouped, text):
        normalized = text.replace(",", "")
    elif re.fullmatch(dot_grouped, text):
        normalized = text.replace(".", "").replace(",", ".")
    else:
        return None
    try:
        amount = Decimal(normalized)
        if not amount.is_finite() or amount <= 0:
            return None
        return amount.quantize(Decimal("0.01"))
    except InvalidOperation:
        return None
def _lex(raw:str,table:dict[str,set[str]])->str|None:
    c={str(raw or "").strip().lower()}
    if not c or "" in c:return None
    for k,a in table.items():
        if c & {x.lower() for x in a}:return k
    return None
def _currency(raw:str)->str|None:
    c={str(raw or "").strip().upper()}
    for k,a in _CURRENCY.items():
        if c & {x.upper() for x in a}:return k
    return None

def _visible_amount(raw: str):
    """Separate visible direction/currency decorations without repairing digits."""
    text = str(raw or "").strip()
    currency = None
    tokens = sorted((token for aliases in _CURRENCY.values() for token in aliases), key=len, reverse=True)
    for token in tokens:
        if text.upper().endswith(token.upper()):
            currency = _currency(token)
            text = text[:-len(token)].strip()
            break
    direction = None
    if text.startswith(("−", "-", "+")):
        direction = "expense" if text[0] in ("−", "-") else "income"
        text = text[1:].strip()
    return parse_raw_amount(text), currency, direction

def reconcile_extractions(passes:tuple[ExtractionPass,...],*,today:Any=None)->ReconciliationResult:
    findings = []
    if any(extraction.mode not in {'single_tx', 'receipt'} for extraction in passes):
        findings.append('non_single_transaction_layout')
    full = [extraction for extraction in passes if extraction.source.startswith('full-')]
    receipt_document = len(full) >= 2 and all(item.mode == "receipt" for item in full)
    if len({extraction.mode for extraction in full}) > 1:
        findings.append('document_layout_conflict')
    if len({extraction.transaction_date for extraction in full}) > 1:
        findings.append('transaction_date_conflict')
    merchants = {' '.join(str(extraction.merchant or '').casefold().split()) for extraction in full}
    if len(merchants) > 1:
        findings.append('merchant_conflict')
    if any(extraction.mode == 'receipt' for extraction in full):
        if len({extraction.receipt_total for extraction in full}) > 1:
            findings.append('receipt_total_conflict')
        merchants = {' '.join(str(extraction.merchant or '').casefold().split()) for extraction in full}
        if len(merchants) > 1:
            findings.append('receipt_merchant_conflict')
        items = [tuple((' '.join(str(row.get('name') or '').casefold().split()), row.get('amount'))
                       for row in extraction.line_items) for extraction in full]
        if any(rows != items[0] for rows in items[1:]):
            findings.append('receipt_items_conflict')
    sources = [extraction.source.strip() for extraction in passes]
    if len(set(sources) - {""}) < 2:
        findings.append("insufficient_distinct_sources")
    if any(source.startswith("full-") for source in sources) and len([source for source in sources if source.startswith("full-")]) < 2:
        findings.append("insufficient_independent_full_passes")
    if any(not source for source in sources) or len(sources) != len(set(sources)):
        findings.append("invalid_source")
    identifiers = [item.id for extraction in passes for item in extraction.evidence]
    if any(not identifier.strip() for identifier in identifiers) or len(identifiers) != len(set(identifiers)):
        findings.append("invalid_evidence_ids")

    values = {field: set() for field in ("amount", "currency", "status", "type")}
    evidence_groups = {field: {} for field in values}
    amount_currencies, amount_directions = set(), set()
    for extraction in passes:
        for field in values:
            items = [item for item in extraction.evidence if item.field == field]
            # Crops are supplemental views: a field outside a crop is absent,
            # not contradictory evidence. Full-image passes still must each
            # provide every required field.
            if not items and not extraction.source.startswith("crop-") and not (receipt_document and field in {"status", "type"}):
                findings.append(field + "_unknown")
            for item in items:
                raw, claimed = item.raw_text.strip(), item.value.strip()
                if field == "amount":
                    parsed, decoration_currency, direction = _visible_amount(raw)
                    if decoration_currency:
                        amount_currencies.add(decoration_currency)
                    if direction:
                        amount_directions.add(direction)
                    try:
                        claimed_matches = bool(re.fullmatch(r"[0-9]+(?:\.[0-9]{1,2})?", claimed)) and parsed is not None and Decimal(claimed) == parsed
                    except InvalidOperation:
                        claimed_matches = False
                elif field == "currency":
                    parsed = _currency(raw)
                    claimed_matches = parsed == claimed.upper()
                else:
                    parsed = _lex(raw, _STATUS if field == "status" else _TYPE)
                    claimed_matches = parsed == claimed.lower()
                if parsed is None or not claimed or not claimed_matches:
                    findings.append(field + "_evidence_invalid")
                if parsed is not None:
                    # IDs are pass-local. Reconcile only evidence that points at
                    # the same visible transaction region/raw token; unrelated
                    # evidence must remain a conflict, never a vote.
                    identity = (field, " ".join(raw.lower().split()), (item.region or "").strip())
                    evidence_groups[field].setdefault(identity, set()).add(parsed)
    for field, groups in evidence_groups.items():
        candidates = set().union(*groups.values()) if groups else set()
        values[field] = candidates
        if any(len(group) != 1 for group in groups.values()) or len(groups) > 1 or len(candidates) != 1:
            findings.append(field + "_conflict")
    if not receipt_document and values["status"] != {"completed"}:
        findings.append("non_completed_status")
    if not receipt_document and (not values["type"] or not values["type"].issubset({"expense", "income"})):
        findings.append("non_financial_type")
    if amount_currencies and amount_currencies != values["currency"]:
        findings.append("amount_currency_conflict")
    if amount_directions and amount_directions != values["type"]:
        findings.append("amount_direction_conflict")

    def single(field):
        candidates = values[field]
        return next(iter(candidates)) if len(candidates) == 1 else None

    findings = tuple(dict.fromkeys(findings))
    accepted = not findings
    representative = next((item for item in passes if item.source.startswith("full-") and (item.merchant or item.transaction_date or item.line_items)), passes[0] if passes else None)
    # Receipt classification needs two independent full-document views. Metadata
    # such as a merchant or a total must never bypass rejected financial evidence.
    receipt_document = len(full) >= 2 and all(item.mode == "receipt" for item in full)
    excluded_receipt = any(
        (item.field == "status" and any(_lex(token, _STATUS) in {"declined", "refunded", "pending"} for token in (item.raw_text, item.value)))
        or (item.field == "type" and any(_lex(token, _TYPE) in {"income", "transfer", "own_account_transfer", "refund"} for token in (item.raw_text, item.value)))
        for extraction in passes for item in extraction.evidence
    )
    if receipt_document and excluded_receipt:
        findings = (*findings, "excluded_receipt")
        accepted = False
    if receipt_document and any(item.mode != "receipt" for item in passes):
        # A supplemental crop can omit the receipt layout. Two agreeing full
        # views may still open a blank manual form, never accepted evidence.
        findings = (*findings, "receipt_crop_layout_conflict")
        accepted = False
    trace = {"stage":"reconciliation", "pass_count":len(passes), "full_pass_count":len(full), "finding_count":len(findings),
             "document_mode":"receipt" if receipt_document else "unsupported",
             "receipt_review_only":receipt_document and not excluded_receipt}
    return ReconciliationResult(accepted, bool(findings), single("amount"), single("currency"), single("status"), single("type"), tuple(identifiers) if accepted else (), findings, trace, representative.transaction_date if representative else today, representative.merchant if representative and "merchant_conflict" not in findings else None, "L1" if accepted else None, representative.line_items if representative else (), representative.receipt_total if representative else None, tuple(item.raw_evidence or {} for item in passes))