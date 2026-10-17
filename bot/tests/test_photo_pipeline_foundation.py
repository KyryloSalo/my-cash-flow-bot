import unittest
from decimal import Decimal
from photo_pipeline import Evidence, ExtractionPass, PipelineConfig, build_cache_key, parse_raw_amount, reconcile_extractions

def ev(i, field, raw, value): return Evidence(i, field, raw, value)
def good(source, n): return ExtractionPass(source, (ev(f"a{n}","amount","1 234,50","1234.50"), ev(f"c{n}","currency","UAH","UAH"), ev(f"s{n}","status","Оплачено","completed"), ev(f"t{n}","type","Оплата","expense")))

class PhotoPipelineFoundationTests(unittest.TestCase):
    def test_accepts_two_distinct_sources_and_exact_evidence(self):
        r=reconcile_extractions((good("full",1),good("tile",2)))
        self.assertTrue(r.accepted); self.assertEqual(r.amount,Decimal("1234.50")); self.assertEqual(r.currency,"UAH"); self.assertEqual(r.transaction_type,"expense")
        self.assertEqual(r.evidence_ids,("a1","c1","s1","t1","a2","c2","s2","t2"))
    def test_digit_substitution_conflicts(self):
        p=(good("full",1), ExtractionPass("tile", (ev("a2","amount","54 000","54000"),)))
        r=reconcile_extractions(p); self.assertFalse(r.accepted); self.assertIn("amount_conflict",r.findings)
    def test_requires_second_distinct_pass(self):
        r=reconcile_extractions((good("full",1),)); self.assertIn("insufficient_distinct_sources",r.findings)
    def test_rejects_arbitrary_currency_and_unknown_evidence(self):
        p=(ExtractionPass("a",(ev("a","amount","10.00","10"),ev("c","currency","XYZ","XYZ"),ev("s","status","maybe","completed"),ev("t","type","transfer","expense"))),good("b",2))
        r=reconcile_extractions(p); self.assertFalse(r.accepted); self.assertIn("currency_evidence_invalid",r.findings); self.assertIn("status_evidence_invalid",r.findings); self.assertIn("type_conflict",r.findings)
    def test_empty_second_pass_and_malformed_amount_fail(self):
        r=reconcile_extractions((good("a",1),ExtractionPass("b"))); self.assertFalse(r.accepted); self.assertIn("amount_unknown",r.findings); self.assertTrue(r.requires_review)
        self.assertIsNone(parse_raw_amount("NaN")); self.assertIsNone(parse_raw_amount("0")); self.assertEqual(parse_raw_amount("1 234,50"),Decimal("1234.50"))
    def test_transfer_and_refund_rejected(self):
        p=(ExtractionPass("a",(ev("a","amount","10","10"),ev("c","currency","USD","USD"),ev("s","status","completed","completed"),ev("t","type","transfer","transfer"))),good("b",2))
        r=reconcile_extractions(p); self.assertIn("type_conflict",r.findings)
    def test_cache_key(self):
        c=PipelineConfig(preprocessing_version="p2",prompt_version="q",schema_version="s"); self.assertNotEqual(build_cache_key(b"x",config=c,model="m",detail="low"),build_cache_key(b"x",config=c,model="m",detail="high"))

    def test_missing_crop_fields_are_supplemental_not_conflicts(self):
        first, second = good("full-a", 1), good("full-b", 2)
        crop = ExtractionPass("crop-0", tuple(Evidence("crop-amount", item.field, item.raw_text, item.value, item.region) for item in first.evidence if item.field == "amount"))
        result = reconcile_extractions((first, second, crop))
        self.assertTrue(result.accepted)
    def test_signed_amount_with_currency_decoration_is_visible_evidence(self):
        p=(good("full-a",1), good("full-b",2))
        p=tuple(ExtractionPass(x.source, tuple(ev(e.id,e.field, "−43,69 UAH" if e.field=="amount" else e.raw_text, "43.69" if e.field=="amount" else e.value) for e in x.evidence)) for x in p)
        r=reconcile_extractions(p)
        self.assertTrue(r.accepted)
        self.assertEqual(r.amount, Decimal("43.69"))

    def test_ukrainian_purchase_label_is_expense_but_transfer_stays_rejected(self):
        p=tuple(ExtractionPass(x.source, tuple(ev(e.id,e.field, "Покупка" if e.field=="type" else e.raw_text, "expense" if e.field=="type" else e.value) for e in x.evidence)) for x in (good("full-a",1),good("full-b",2)))
        self.assertEqual(reconcile_extractions(p).transaction_type, "expense")
        transfer=tuple(ExtractionPass(x.source, tuple(ev(e.id,e.field, "Переказ" if e.field=="type" else e.raw_text, "transfer" if e.field=="type" else e.value) for e in x.evidence)) for x in (good("full-a",1),good("full-b",2)))
        self.assertFalse(reconcile_extractions(transfer).accepted)

if __name__ == "__main__": unittest.main()
