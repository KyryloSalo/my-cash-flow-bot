from datetime import timedelta
import hashlib
import hmac
import json
import re
import time
from unittest.mock import patch
from urllib.parse import urlencode

from django.test import TestCase, override_settings
from django.utils import timezone

from miniapp.funnel import ACQUISITION_SESSION_KEY
from miniapp.models import AcquisitionSession, PartnerLink, PartnerTrialClaim
from subscriptions.models import Subscription
from users.models import TelegramUser


@override_settings(TELEGRAM_BOT_TOKEN="partner-trial-test-fixture")
class PartnerRegistrationShellTests(TestCase):
    def setUp(self):
        self.link = PartnerLink.objects.create(
            name='Manual partner', code='manual51', trial_days=51,
            utm_source='creator', destination=PartnerLink.Destination.APP,
        )

    def shell_offer(self, response):
        self.assertEqual(response.status_code, 200)
        script = re.search(
            r'<script id="registration-partner-offer" type="application/json">(.*?)</script>',
            response.content.decode(), re.DOTALL,
        )
        self.assertIsNotNone(script)
        return json.loads(script.group(1))

    def _register(self, user_id=9100051):
        payload = {"auth_date": str(int(time.time())), "user": json.dumps({"id": user_id, "first_name": "Partner fixture"})}
        secret = hmac.new(b"WebAppData", b"partner-trial-test-fixture", hashlib.sha256).digest()
        check = "\n".join(f"{key}={value}" for key, value in sorted(payload.items()))
        payload["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        return self.client.post("/app/api/auth/telegram", {"init_data": urlencode(payload)}, content_type="application/json")

    def test_manual_link_renders_exact_offer_before_authentication(self):
        redirected = self.client.get('/app/r/manual51')
        self.assertRedirects(redirected, '/app/', fetch_redirect_response=False)
        page = self.client.get('/app/')
        self.assertEqual(self.shell_offer(page), {
            'code': 'manual51', 'name': 'Manual partner', 'trial_days': 51,
        })
        self.assertIn('no-store', page.headers['Cache-Control'])
        self.assertNotIn('miniapp_tg_user_id', self.client.session)

    def test_expired_acquisition_never_promises_stale_days(self):
        acquisition = AcquisitionSession.objects.create(
            first_partner_link=self.link, last_partner_link=self.link,
            expires_at=timezone.now() - timedelta(seconds=1),
        )
        session = self.client.session
        session[ACQUISITION_SESSION_KEY] = str(acquisition.pk)
        session.save()
        self.assertIsNone(self.shell_offer(self.client.get('/app/')))

    def test_invalid_or_unknown_manual_hash_does_not_grant_offer(self):
        for code in ('unknown51', 'UPPER', 'ab'):
            with self.subTest(code=code):
                self.assertEqual(self.client.get('/app/r/' + code).status_code, 404)
        self.assertIsNone(self.shell_offer(self.client.get('/app/')))

    def test_deactivated_offer_is_not_promised(self):
        self.client.get('/app/r/manual51')
        self.link.is_active = False
        self.link.save(update_fields=['is_active'])
        self.assertIsNone(self.shell_offer(self.client.get('/app/')))

    def test_malformed_acquisition_session_falls_back_without_server_error(self):
        session = self.client.session
        session[ACQUISITION_SESSION_KEY] = 'not-a-uuid'
        session.save()
        self.assertIsNone(self.shell_offer(self.client.get('/app/')))

    def test_signed_new_registration_reserves_exactly_51_days_without_access(self):
        self.client.get('/app/r/manual51')
        self.assertEqual(self._register().status_code, 200)
        claim = PartnerTrialClaim.objects.get(user_id=9100051)
        self.assertEqual(claim.trial_days, 51)
        self.assertIsNone(claim.activated_at)
        self.assertFalse(Subscription.objects.filter(user_id=9100051).exists())
        for path in ('flow', 'accounts/options', 'transactions/options', 'history'):
            with self.subTest(path=path):
                self.assertEqual(self.client.get('/app/api/' + path).status_code, 403)
        self.assertEqual(self.shell_offer(self.client.get('/app/'))['trial_days'], 51)
        status = self.client.get('/app/api/onboarding').json()['onboarding']
        self.assertEqual(status['partner_offer']['trial_days'], 51)

    def test_failed_reservation_rolls_back_the_new_registration(self):
        self.client.get('/app/r/manual51')
        with patch('miniapp.partner_offers.PartnerTrialClaim.objects.create', side_effect=RuntimeError('reservation unavailable')):
            self.assertEqual(self._register().status_code, 503)
        self.assertFalse(TelegramUser.objects.filter(tg_user_id=9100051).exists())
        self.assertEqual(self._register().status_code, 200)
        self.assertEqual(PartnerTrialClaim.objects.get(user_id=9100051).trial_days, 51)
        self.assertFalse(Subscription.objects.filter(user_id=9100051).exists())

    def test_click_offer_survives_admin_day_change_before_registration(self):
        self.client.get('/app/r/manual51')
        self.link.trial_days = 137
        self.link.save(update_fields=['trial_days'])
        self.assertEqual(self.shell_offer(self.client.get('/app/'))['trial_days'], 51)
        self.assertEqual(self._register().status_code, 200)
        self.assertEqual(PartnerTrialClaim.objects.get(user_id=9100051).trial_days, 51)
        self.assertFalse(Subscription.objects.filter(user_id=9100051).exists())

    def test_partner_cta_funnel_session_bootstraps_exact_7889_days(self):
        PartnerLink.objects.create(
            name="Launch partner", code="launch7889", trial_days=7889,
            utm_source="launch", destination=PartnerLink.Destination.APP,
        )
        self.assertEqual(self.client.get("/app/r/launch7889").status_code, 302)
        funnel = self.client.get("/app/api/funnel/session", {"landing_variant": "partner-cta"})
        self.assertEqual(funnel.status_code, 200)
        self.assertEqual(funnel.json()["partner_offer"]["trial_days"], 7889)
        self.assertEqual(self._register(user_id=91007889).status_code, 200)
        claim = PartnerTrialClaim.objects.get(user_id=91007889)
        self.assertEqual(claim.trial_days, 7889)
        self.assertIsNone(claim.activated_at)
        self.assertFalse(Subscription.objects.filter(user_id=91007889).exists())
        self.assertEqual(self.client.get("/app/api/onboarding").json()["onboarding"]["partner_offer"]["trial_days"], 7889)
