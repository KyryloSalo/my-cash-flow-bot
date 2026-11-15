"""Explicit local provider seams and real-browser lifecycle acceptance helpers.

Imported only by billing_lifecycle_acceptance.py; never by application startup.
All provider responses here are deliberately synthetic, not provider evidence.
"""
from __future__ import annotations

LIFECYCLE_USER = 788882


def install_provider_seams(origin):
    from subscriptions import billing
    from urllib.parse import urlencode, urlsplit, parse_qs

    assert origin.startswith('http://127.0.0.1:')
    counter = {'invoice_creations': 0}

    def create_invoice(payload):
        assert payload['amount'] == 100
        assert payload['saveCardData']['saveCard'] is True
        counter['invoice_creations'] += 1
        assert counter['invoice_creations'] <= 2, 'duplicate synthetic provider invoice'
        redirect = payload['redirectUrl']
        assert redirect.startswith(origin + '/billing/mono/return?')
        signed = parse_qs(urlsplit(redirect).query)['return_token'][0]
        assert billing.parse_bind_return_token(signed)['app_login_token']
        return {'invoiceId': 'synthetic-acceptance-bind-' + payload['merchantPaymInfo']['reference'].split(':')[1] + '-' + str(counter['invoice_creations']),
                'pageUrl': origin + '/app/?' + urlencode({'synthetic-checkout': counter['invoice_creations'], 'provider_return': redirect})}

    billing.create_invoice = create_invoice
    billing.fetch_invoice_status = lambda *args, **kwargs: {}
    billing.fetch_wallet_cards = lambda *args, **kwargs: {}
    billing.send_bind_activation_notification = lambda *args, **kwargs: None
    return counter


SEED_LIFECYCLE = '''
from users.models import TelegramUser
from subscriptions.models import Payment
from miniapp.models import PartnerLink, PartnerTrialClaim
from accounts.models import Account
from categories.models import Category
from django.utils import timezone
u=TelegramUser.objects.create(tg_user_id=788882, first_name='Lifecycle', lang='en', base_currency='UAH', onboarding_completed=True, onboarding_version=3, start_date=timezone.localdate())
Account.objects.create(tg_user=u,label='Lifecycle synthetic',currency='UAH')
for kind in ['income','expense']:
    Category.objects.create(user=u,tg_user=u,type=kind,name='Lifecycle '+kind)
link=PartnerLink.objects.create(name='Acceptance original 51 days',code='acceptance-original-51',trial_days=51)
PartnerTrialClaim.objects.create(user=u,partner_link=link,offer_id=f'partner_link:{link.pk}',code=link.code,name=link.name,trial_days=51)
assert not Payment.objects.filter(user=u).exists()
print('LIFECYCLE_FIXTURE: original partner claim 51 days; no payment or entitlement')
'''

VERIFY_CHECKOUT = '''
from subscriptions.models import Payment, BillingConsent, BillingProfile, Subscription
from miniapp.models import PartnerTrialClaim, PartnerLink
p=Payment.objects.get(user_id=788882)
c=BillingConsent.objects.get(user_id=788882)
claim=PartnerTrialClaim.objects.get(user_id=788882)
assert p.kind=='bind' and p.status=='pending'
assert c.payment_id==p.pk and c.trial_days==51 and c.offer_id==claim.offer_id
assert p.raw_payload['bind_context']['trial_days']==51
assert p.raw_payload['bind_context']['partner_claim_id']==claim.pk
assert not Subscription.objects.filter(user_id=788882).exists()
assert BillingProfile.objects.filter(user_id=788882).count()==1
PartnerLink.objects.filter(pk=claim.partner_link_id).update(trial_days=99)
print('CHECKOUT_PASS: browser consent linked to one canonical invoice; original 51-day snapshot persisted; live offer changed to 99')
'''

EVENT_SETUP = '''
from unittest.mock import patch
from subscriptions import billing
from subscriptions.models import Payment, Subscription, SubscriptionEvent, BillingProfile, BillingConsent
from miniapp.models import PartnerTrialClaim
from django.utils import timezone
from datetime import timedelta
p=Payment.objects.get(user_id=788882)
event={'invoiceId':p.provider_payment_id,'status':'success','amount':100,'ccy':980,'modifiedDate':timezone.now().isoformat()}
'''

PAID_NO_TOKEN = EVENT_SETUP + '''
with patch('subscriptions.billing.fetch_invoice_status',return_value={}), patch('subscriptions.billing.fetch_wallet_cards',return_value={}), patch('subscriptions.billing.send_bind_activation_notification'):
    billing.process_monobank_event(event)
p.refresh_from_db()
assert p.status=='paid' and p.subscription_id is None
assert not Subscription.objects.filter(user_id=788882).exists()
assert PartnerTrialClaim.objects.get(user_id=788882).activated_at is None
assert not BillingProfile.objects.get(user_id=788882).card_token
assert Payment.objects.filter(user_id=788882).count()==1
print('PROVIDER_SEAM_PASS: synthetic success without token records paid but grants no access')
'''

LATE_TOKEN = EVENT_SETUP + '''
event['walletData']={'cardToken':'synthetic-local-late-token'}
with patch('subscriptions.billing.fetch_invoice_status',return_value={}), patch('subscriptions.billing.fetch_wallet_cards',return_value={}), patch('subscriptions.billing.send_bind_activation_notification'):
    billing.process_monobank_event(event)
    s=Subscription.objects.get(user_id=788882)
    original=(s.pk,s.started_at,s.expires_at,s.next_charge_at,s.auto_renew)
    billing.process_monobank_event(event)
s.refresh_from_db(); p.refresh_from_db()
assert (s.pk,s.started_at,s.expires_at,s.next_charge_at,s.auto_renew)==original
assert s.trial_days==51 and s.expires_at-s.started_at==timedelta(days=51)
assert not s.auto_renew and s.next_charge_at is None
assert p.subscription_id==s.pk
assert PartnerTrialClaim.objects.get(user_id=788882).activated_at is not None
assert SubscriptionEvent.objects.filter(user_id=788882,event_type='mono_trial_activated').count()==1
assert Payment.objects.filter(user_id=788882).count()==1
assert BillingConsent.objects.get(user_id=788882).payment_id==p.pk
print('ACTIVATION_PASS: late synthetic token activates canonical original 51-day trial exactly once; duplicate replay preserves dates; renewal stays off')
'''

MANUAL_PRESERVED = EVENT_SETUP + '''
m=Subscription.objects.create(user_id=788882,status='manual',source='admin',started_at=timezone.now(),expires_at=timezone.now()+timedelta(days=120),auto_renew=False)
original=(m.pk,m.status,m.source,m.started_at,m.expires_at,m.next_charge_at,m.auto_renew)
event['walletData']={'cardToken':'synthetic-local-late-token'}
with patch('subscriptions.billing.fetch_invoice_status',return_value={}), patch('subscriptions.billing.fetch_wallet_cards',return_value={}), patch('subscriptions.billing.send_bind_activation_notification'):
    billing.process_monobank_event(event)
m.refresh_from_db()
assert (m.pk,m.status,m.source,m.started_at,m.expires_at,m.next_charge_at,m.auto_renew)==original
assert Payment.objects.filter(user_id=788882).count()==1
assert BillingConsent.objects.filter(user_id=788882).count()==1
assert BillingProfile.objects.filter(user_id=788882).count()==1
assert Subscription.objects.filter(user_id=788882).count()==2
assert SubscriptionEvent.objects.filter(user_id=788882,event_type='mono_trial_activated').count()==1
print('MANUAL_PRESERVATION_PASS: duplicate event preserves later manual grant and canonical trial; payment=1 consent=1 profile=1 activation_event=1')
'''


def run_browser_lifecycle(base, manage, report):
    from playwright.sync_api import sync_playwright, expect

    def shell(source):
        for line in manage('shell', '-c', source).splitlines():
            if '_PASS:' in line or line.startswith('LIFECYCLE_FIXTURE:'):
                report(line)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        report('CHROMIUM_VERSION=' + browser.version)
        try:
            context = browser.new_context(service_workers='block')
            context.route('**/*', lambda route: route.continue_() if route.request.url.startswith(base+'/') else route.abort())
            page = context.new_page()
            page.goto(base+'/app/', wait_until='networkidle')
            auth = page.locator('body').get_attribute('data-dev-auth-url')
            response = context.request.post(base + auth, data={})
            assert response.status == 200, 'lifecycle dev auth failed'
            page.reload(wait_until='networkidle')
            # Dismiss only the redundant card nudge, not the real billing gate.
            modal = page.locator('#cardBindingModal')
            if modal.is_visible():
                page.locator('#cardBindingLater').click()
            expect(page.locator('#billingBindBtn')).to_be_visible()
            expect(page.locator('#billingBindBtn')).to_be_disabled()
            page.locator('#billingConsentAccepted').check()
            expect(page.locator('#billingBindBtn')).to_be_enabled()
            with page.expect_response(lambda r: r.request.method == 'POST' and '/api/settings/billing' in r.url) as checkout:
                page.locator('#billingBindBtn').click()
            assert checkout.value.status == 200, 'checkout HTTP status='+str(checkout.value.status)
            page.wait_for_url('**/app/?synthetic-checkout=1&provider_return=*')
            from urllib.parse import parse_qs, urlsplit
            signed_return = parse_qs(urlsplit(page.url).query)['provider_return'][0]
            assert signed_return.startswith(base + '/billing/mono/return?return_token=')
            expect(page.locator('#billingActivationCheck')).to_be_visible()
            expect(page.locator('#billingBindBtn')).to_be_hidden()
            shell(VERIFY_CHECKOUT)
            shell(PAID_NO_TOKEN)
            returned = page.goto(signed_return, wait_until='networkidle')
            assert returned.status == 200
            expect(page.locator('h1')).to_have_text('Payment received')
            expect(page.locator('.message')).to_contain_text('Do not pay again')
            expect(page.locator('form[action*="open-app"]')).to_have_count(0)
            report('SIGNED_RETURN_PASS: actual invoice redirectUrl; paid/no-token honest pending; no app-login handoff')
            page.goto(base+'/app/', wait_until='networkidle')

            def refresh(expected_mode):
                # Await the click-triggered request, not an already-idle document.
                with page.expect_response(lambda r: '/api/settings' in r.url and r.request.method == 'GET') as pending:
                    page.locator('#billingActivationCheck').click()
                response = pending.value
                assert response.status == 200
                body = response.json()
                payload = body.get('data') or body
                assert payload['access']['mode'] == expected_mode, payload['access']
                response.finished()
                diagnostic = payload['acceptance_diagnostic']
                assert diagnostic['invoice_creations'] == 1
                report('LIFECYCLE_SETTINGS mode=' + expected_mode + ' invoice_creations=1')
                page.wait_for_load_state('networkidle')

            refresh('blocked')
            expect(page.locator('#settingsBillingMessage')).to_contain_text('Do not pay again')
            expect(page.locator('#billingContinueBtn')).to_be_hidden()
            expect(page.locator('#billingBindBtn')).to_be_hidden()
            expect(modal).to_be_hidden()
            report('BROWSER_LIFECYCLE_PASS: real consent checkbox/button created checkout; paid/no-token remains blocked with no duplicate charge CTA')
            shell(LATE_TOKEN)
            refresh('active')
            expect(page.locator('#billingActivationCheck')).to_be_hidden()
            expect(page.locator('#billingBindBtn')).to_be_visible()
            expect(modal).to_be_hidden()
            report('BROWSER_LIFECYCLE_PASS: same session refresh observes real PostgreSQL activation and explicit rebind only')
            # A fresh browser has no dev/session auth: exercise the canonical signed
            # app_login_token consumption through the real CSRF-protected form.
            handoff_context = browser.new_context(service_workers='block')
            handoff_context.route('**/*', lambda route: route.continue_() if route.request.url.startswith(base+'/') else route.abort())
            handoff = handoff_context.new_page()
            handoff.goto(signed_return, wait_until='networkidle')
            expect(handoff.locator('h1')).to_have_text('Access activated')
            expect(handoff.locator('input[name=return_token]')).to_have_value(parse_qs(urlsplit(signed_return).query)['return_token'][0])
            with handoff.expect_response(lambda r: '/billing/mono/open-app' in r.url and r.request.method == 'POST') as opened:
                handoff.get_by_role('button', name='Open app', exact=True).click()
            assert opened.value.status == 302
            handoff.wait_for_url(base+'/app/')
            handoff.wait_for_load_state('networkidle')
            state = handoff_context.request.get(base+'/app/api/settings').json()
            assert (state.get('data') or state)['access']['mode'] == 'active'
            assert (state.get('data') or state)['acceptance_diagnostic']['user'] == LIFECYCLE_USER
            assert (state.get('data') or state)['acceptance_diagnostic']['invoice_creations'] == 1
            handoff_context.close()
            report('SIGNED_RETURN_PASS: late-token active return; fresh-browser signed app-login POST handoff authenticates canonical user')
            shell(MANUAL_PRESERVED)
            page.goto(signed_return, wait_until='networkidle')
            expect(page.locator('h1')).to_have_text('Access activated')
            with page.expect_response(lambda r: '/billing/mono/open-app' in r.url and r.request.method == 'POST') as manual_open:
                page.get_by_role('button', name='Open app', exact=True).click()
            assert manual_open.value.status == 302
            page.wait_for_url(base+'/app/')
            report('SIGNED_RETURN_PASS: effective manual access shows active return and same-session app handoff')
            page.reload(wait_until='networkidle')
            # Active users start on dashboard; settings is reached through its real nav button.
            settings_nav = page.locator('[data-screen-target="settings"]')
            if settings_nav.count() and settings_nav.first.is_visible():
                settings_nav.first.click()
            else:
                page.goto(base+'/app/?screen=settings', wait_until='networkidle')
            expect(page.locator('#billingActivationCheck')).to_be_hidden()
            expect(modal).to_be_hidden()
            report('BROWSER_LIFECYCLE_PASS: reload after manual grant/replay does not restore activation gate or binding nudge')
        except Exception as exc:
            report('LIFECYCLE_BROWSER_ERROR: '+type(exc).__name__)
            report('LIFECYCLE_DOM: '+str(page.locator('#billingBindBtn').count())+' bind; '+str(page.locator('#cardBindingModal').is_visible())+' modal')
            raise
        finally:
            browser.close()

SEED_UNPAID = SEED_LIFECYCLE.replace('788882', '788883').replace('acceptance-original-51', 'acceptance-unpaid-51')


def run_unpaid_continuation(base, manage, report):
    """Leave an unpaid checkout, resume exactly it, then fail and legitimately retry."""
    from playwright.sync_api import sync_playwright, expect
    from urllib.parse import parse_qs, urlsplit
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(service_workers='block')
            context.route('**/*', lambda route: route.continue_() if route.request.url.startswith(base+'/') else route.abort())
            page = context.new_page()
            page.goto(base+'/app/', wait_until='networkidle')
            auth = page.locator('body').get_attribute('data-dev-auth-url')
            auth_result = context.request.post(base+auth, data={})
            assert auth_result.status == 200, 'unpaid dev auth HTTP status='+str(auth_result.status)
            page.reload(wait_until='networkidle')
            if page.locator('#cardBindingModal').is_visible():
                page.locator('#cardBindingLater').click()
            page.locator('#billingConsentAccepted').check()
            with page.expect_response(lambda r: r.request.method == 'POST' and '/api/settings/billing' in r.url) as created:
                page.locator('#billingBindBtn').click()
            assert created.value.status == 200, 'unpaid checkout HTTP status='+str(created.value.status)
            page.wait_for_url('**/app/?synthetic-checkout=1&provider_return=*')
            original_checkout = page.url
            signed_return = parse_qs(urlsplit(original_checkout).query)['provider_return'][0]
            page.goto(base+'/app/', wait_until='networkidle')
            expect(page.locator('#billingBindBtn')).to_be_hidden()
            expect(page.locator('#billingContinueBtn')).to_be_visible()
            with page.expect_navigation(wait_until='networkidle'):
                page.locator('#billingContinueBtn').click()
            assert page.url == original_checkout, 'continuation must reopen the exact original invoice'
            output = manage('shell', '-c', """
from subscriptions.models import Payment, BillingConsent, Subscription
p=Payment.objects.get(user_id=788883)
assert p.status=='pending'
assert BillingConsent.objects.filter(user_id=788883).count()==1
assert not Subscription.objects.filter(user_id=788883).exists()
print('UNPAID_DATABASE_PASS: resumed same canonical pending invoice; payment=1 consent=1 entitlement=0')
""")
            for line in output.splitlines():
                if '_PASS:' in line: report(line)
            state = context.request.get(base+'/app/api/settings').json()
            assert (state.get('data') or state)['acceptance_diagnostic']['invoice_creations'] == 1
            report('UNPAID_BROWSER_PASS: leave-before-payment and real Continue click reopen identical checkout; provider invoice creations=1')
            output = manage('shell', '-c', """
from unittest.mock import patch
from subscriptions import billing
from subscriptions.models import Payment
from django.utils import timezone
p=Payment.objects.get(user_id=788883)
with patch('subscriptions.billing.send_bind_activation_notification'):
    billing.process_monobank_event({'invoiceId':p.provider_payment_id,'status':'failure','amount':100,'ccy':980,'modifiedDate':timezone.now().isoformat(),'failureReason':'synthetic definitive decline'})
p.refresh_from_db()
assert p.status=='failed'
print('FAILURE_DATABASE_PASS: synthetic provider definitive failure persisted')
""")
            for line in output.splitlines():
                if '_PASS:' in line: report(line)
            page.goto(signed_return, wait_until='networkidle')
            expect(page.locator('h1')).to_have_text('Payment not confirmed')
            page.goto(base+'/app/', wait_until='networkidle')
            if page.locator('#cardBindingModal').is_visible():
                page.locator('#cardBindingLater').click()
            expect(page.locator('#billingContinueBtn')).to_be_hidden()
            expect(page.locator('#billingBindBtn')).to_be_visible()
            page.locator('#billingConsentAccepted').check()
            with page.expect_response(lambda r: r.request.method == 'POST' and '/api/settings/billing' in r.url) as retry:
                page.locator('#billingBindBtn').click()
            assert retry.value.status == 200
            page.wait_for_url('**/app/?synthetic-checkout=2&provider_return=*')
            assert page.url != original_checkout
            state = context.request.get(base+'/app/api/settings').json()
            assert (state.get('data') or state)['acceptance_diagnostic']['invoice_creations'] == 2
            output = manage('shell', '-c', """
from subscriptions.models import Payment, BillingConsent, BillingProfile, Subscription
payments=list(Payment.objects.filter(user_id=788883).order_by('id'))
assert len(payments)==2 and [p.status for p in payments]==['failed','pending']
assert payments[0].provider_payment_id != payments[1].provider_payment_id
assert BillingConsent.objects.filter(user_id=788883).count()==2
assert BillingConsent.objects.filter(user_id=788883,payment=payments[1]).count()==1
assert BillingProfile.objects.filter(user_id=788883).count()==1
assert not Subscription.objects.filter(user_id=788883).exists()
print('RETRY_DATABASE_PASS: definitive failure permits exactly one new invoice; failed=1 pending=1 consent=2 profile=1 entitlement=0')
""")
            for line in output.splitlines():
                if '_PASS:' in line: report(line)
            report('UNPAID_BROWSER_PASS: real failed signed return and consent/retry clicks create a distinct legitimate invoice only after definitive failure')
        finally:
            browser.close()
