import inspect

from django import forms
from django.test import SimpleTestCase

from users.admin import ForceChargeNowForm, TelegramUserAdmin


class UserAdminBillingIntentTests(SimpleTestCase):
    def test_force_charge_confirmation_carries_stable_hidden_intent(self):
        self.assertIn("intent_key", ForceChargeNowForm.base_fields)
        self.assertIsInstance(ForceChargeNowForm.base_fields["intent_key"].widget, forms.HiddenInput)
        source = inspect.getsource(TelegramUserAdmin.force_charge_now_view)
        self.assertIn('cleaned_data.get("intent_key")', source)
        self.assertIn("intent_key=intent_key", source)
