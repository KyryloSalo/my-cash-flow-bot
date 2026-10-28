import unittest
from types import SimpleNamespace

from screenshot_review import screenshot_review_metadata, unresolved_photo_currency


class ScreenshotReviewMetadataTests(unittest.TestCase):
    def test_conflict_evidence_survives_without_raw_provider_storage(self):
        analysis = SimpleNamespace(
            needs_review=True, field_findings=['currency_conflict'],
            currency_candidates=['EUR', 'UAH'], raw_response=None,
        )
        metadata = screenshot_review_metadata(analysis)
        self.assertTrue(metadata['needs_review'])
        self.assertEqual(metadata['field_findings'], ['currency_conflict'])
        self.assertEqual(metadata['currency_candidates'], ['EUR', 'UAH'])
        self.assertNotIn('raw_response', metadata)
        metadata['field_findings'].append('changed')
        self.assertEqual(analysis.field_findings, ['currency_conflict'])

    def test_normal_analysis_has_no_invented_conflict(self):
        metadata = screenshot_review_metadata(SimpleNamespace(needs_review=False, field_findings=()))
        self.assertEqual(metadata, {'needs_review': False, 'field_findings': [], 'field_evidence': {}, 'currency_candidates': []})

    def test_photo_missing_currency_is_blocked_even_after_account_selection(self):
        flow = {
            'origin': 'screenshot', 'draft_source': 'screenshot',
            'draft_metadata': {'needs_review': True, 'field_findings': ['currency_unknown']},
            'tx': {'currency': 'EUR', 'currency_explicit': False, 'account_id': 9},
        }
        self.assertTrue(unresolved_photo_currency(flow))

    def test_photo_conflict_is_blocked_and_facts_remain_present(self):
        flow = {
            'origin': 'screenshot', 'draft_source': 'screenshot',
            'draft_metadata': {'needs_review': True, 'field_findings': ['currency_conflict'],
                               'currency_candidates': ['EUR', 'UAH']},
            'tx': {'currency': 'UAH', 'currency_explicit': True},
        }
        self.assertTrue(unresolved_photo_currency(flow))
        self.assertEqual(flow['draft_metadata']['currency_candidates'], ['EUR', 'UAH'])

    def test_valid_photo_and_manual_text_are_not_blocked(self):
        self.assertFalse(unresolved_photo_currency({
            'origin': 'screenshot', 'draft_source': 'screenshot',
            'draft_metadata': {'needs_review': False, 'field_findings': []},
            'tx': {'currency': 'UAH', 'currency_explicit': True},
        }))
        self.assertFalse(unresolved_photo_currency({
            'origin': 'text', 'tx': {'currency': 'UAH', 'currency_explicit': False},
        }))


if __name__ == '__main__':
    unittest.main()
