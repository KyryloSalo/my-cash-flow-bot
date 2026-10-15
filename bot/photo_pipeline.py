"""Shadow-only, fail-closed photo evidence pipeline."""
from __future__ import annotations
import hashlib, re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any
_CURRENCY={"UAH":{"UAH","ГРН","₴"},"EUR":{"EUR","€","EURO"},"USD":{"USD","US$"},"TRY":{"TRY","TL","₺","LIRA","LIRAS"}}
_STATUS={"completed":{"completed","paid","successful","успішно","оплачено","başarılı"},"declined":{"declined","rejected","failed","відхилено","отклонен","başarısız"},"refunded":{"refunded","refund","reversed","повернення","iade"}}
_TYPE={"expense":{"expense","purchase","payment","витрата","оплата","harcama"},"income":{"income","credit","дохід","зарахування","gelir"},"transfer":{"transfer","переказ","перевод","havale"},"own_account_transfer":{"own account","власний рахунок","свій рахунок","kendi hesabı"},"refund":{"refund","refunded","повернення","iade"}}
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
@dataclass(frozen=True)
class PipelineConfig: preprocessing_version:str="photo-preprocess-v1"; prompt_version:str="photo-evidence-v1"; schema_version:str="photo-schema-v1"
@dataclass(frozen=True)
class CacheKey:
    original_sha256:str; preprocessing_version:str; model:str; detail:str; prompt_version:str; schema_version:str
    def as_string(self)->str:return ":".join((self.original_sha256,self.preprocessing_version,self.model,self.detail,self.prompt_version,self.schema_version))
@dataclass(frozen=True)
class ReconciliationResult:
    accepted:bool; requires_review:bool; amount:Decimal|None; currency:str|None; status:str|None; transaction_type:str|None; evidence_ids:tuple[str,...]; findings:tuple[str,...]; trace:dict[str,Any]; transaction_date:Any=None; merchant:str|None=None; receipt_level:str|None=None; line_items:tuple[dict[str,Any],...]=(); receipt_total:Decimal|None=None; raw_evidence:tuple[dict[str,Any],...]=()
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
def reconcile_extractions(passes:tuple[ExtractionPass,...],*,today:Any=None)->ReconciliationResult:
    findings = []
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
    for extraction in passes:
        for field in values:
            items = [item for item in extraction.evidence if item.field == field]
            if not items and not extraction.source.startswith("crop-"):
                findings.append("pass_evidence_incomplete")
            for item in items:
                raw, claimed = item.raw_text.strip(), item.value.strip()
                if field == "amount":
                    parsed = parse_raw_amount(raw)
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
                    values[field].add(parsed)
    for field, candidates in values.items():
        if len(candidates) != 1:
            findings.append(field + "_conflict")
    if values["status"] != {"completed"}:
        findings.append("non_completed_status")
    if not values["type"] or not values["type"].issubset({"expense", "income"}):
        findings.append("non_financial_type")

    def single(field):
        candidates = values[field]
        return next(iter(candidates)) if len(candidates) == 1 else None

    findings = tuple(dict.fromkeys(findings))
    accepted = not findings
    representative = next((item for item in passes if item.source.startswith("full-") and (item.merchant or item.transaction_date or item.line_items)), passes[0] if passes else None)
    return ReconciliationResult(accepted, not accepted, single("amount"), single("currency"), single("status"), single("type"), tuple(identifiers) if accepted else (), findings, {"stage":"reconciliation", "pass_count":len(passes), "full_pass_count":len([p for p in passes if p.source.startswith("full-")]), "finding_count":len(findings)}, representative.transaction_date if representative else today, representative.merchant if representative else None, "L1" if accepted else None, representative.line_items if representative else (), representative.receipt_total if representative else None, tuple(item.raw_evidence or {} for item in passes))