from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch
import json

from django.test import RequestFactory, SimpleTestCase

from .views import ai_image_batch_item


class ReviewEditValidationTests(SimpleTestCase):
    def submit(self, changes):
        batch = {"batch_id": "batch-1", "items": [{"id": "item-1", "status": "pending", "suggestion": {"amount": "12.50", "comment": "Original"}}]}
        original = deepcopy(batch)
        request = RequestFactory().post("/review", data=json.dumps({"batch_id": "batch-1", "item_id": "item-1", "action": "review", **changes}), content_type="application/json")
        with patch("miniapp.views._authorized_write_user_or_blocked", return_value=(SimpleNamespace(), None)), patch("miniapp.views._session_ai_image_batch", return_value=batch), patch("miniapp.views._find_ai_image_batch_item", return_value=batch["items"][0]), patch("miniapp.views._save_session_ai_image_batch") as save, patch("miniapp.views._ai_image_batch_payload", return_value=batch):
            response = ai_image_batch_item(request)
        return response, batch, original, save

    def test_invalid_amount_rejects_entire_edit_without_mutation(self):
        for value in ["", "bad", "NaN", "Infinity", "-1", "0", "1.001", "1000000000000", None, 12]:
            with self.subTest(value=value):
                response, batch, original, save = self.submit({"amount": value, "comment": "Changed"})
                self.assertEqual(response.status_code, 400)
                self.assertEqual(batch, original)
                save.assert_not_called()

    def test_invalid_comment_rejects_without_mutation(self):
        for value in [None, 12, "x" * 501]:
            with self.subTest(value=value):
                response, batch, original, save = self.submit({"comment": value})
                self.assertEqual(response.status_code, 400)
                self.assertEqual(batch, original)
                save.assert_not_called()

    def test_valid_edit_is_saved(self):
        response, batch, _, save = self.submit({"amount": "15,25", "comment": " Changed "})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(batch["items"][0]["suggestion"], {"amount": "15.25", "comment": "Changed"})
        save.assert_called_once()
