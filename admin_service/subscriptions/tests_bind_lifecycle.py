"""Synthetic PostgreSQL regressions; provider seams only are mocked."""
from datetime import timedelta
from unittest.mock import patch
from django.test import TestCase, override_settings
from django.utils import timezone
from miniapp.models import PartnerLink, PartnerTrialClaim
from subscriptions import billing
from subscriptions.models import Payment, Subscription, SubscriptionEvent
from users.models import TelegramUser

@override_settings(TELEGRAM_BOT_TOKEN="", MONO_RENEWAL_AMOUNT=49900)
class BindLifecycleTests(TestCase):
    def test_worker_skips_ineligible_users_before_batch_limit(self):
        self.make_paid_pending()
        self.payment.raw_payload["activation_reconciliation"]={"attempts":8}
        self.payment.save()
        for index, state in enumerate(("cooldown", "manual", "due")):
            user=TelegramUser.objects.create(tg_user_id=84561100+index)
            claim=PartnerTrialClaim.objects.create(user=user,partner_link=self.claim.partner_link,
                offer_id=self.claim.offer_id,code="fair",name="Fair",trial_days=51)
            payment=Payment.objects.create(user=user,provider="monobank",provider_payment_id="fair-"+state,
                amount="1.00",currency="UAH",kind=Payment.Kind.BIND,status=Payment.Status.PAID,
                raw_payload={"bind_context":{"trial_granted":True,"partner_claim_id":claim.pk,"trial_days":51},
                    "applied_effects":{"paid":"recorded"}})
            if state=="cooldown":
                payment.raw_payload["activation_reconciliation"]={"attempts":1,
                    "next_retry_at":(timezone.now()+timedelta(hours=1)).isoformat()}
                payment.save()
            elif state=="manual":
                Subscription.objects.create(user=user,status=Subscription.Status.MANUAL,
                    source=Subscription.Source.ADMIN,expires_at=timezone.now()+timedelta(days=90))
            else:
                due_claim=claim
        with patch("subscriptions.billing.fetch_invoice_status",return_value={**self.event(),"invoiceId":"fair-due"}) as fetch, patch(
                "subscriptions.billing._resolve_paid_save_card_payload",return_value=("synthetic",{})):
            result=billing.reconcile_pending_monobank_binds(limit=1)
        due_claim.refresh_from_db()
        self.assertIsNotNone(due_claim.activated_at)
        fetch.assert_called_once_with("fair-due")
        self.assertEqual(result["operator_required"],1)

    def make_tokenless_later_paid(self):
        self.make_paid_pending()
        later=Payment.objects.create(user=self.user,provider="monobank",provider_payment_id="late-B",
            amount="1.00",currency="UAH",kind=Payment.Kind.BIND,status=Payment.Status.PENDING,
            raw_payload={"bind_context":{"mode":"rebind","trial_granted":False}})
        event={**self.event(),"invoiceId":later.provider_payment_id}
        with patch("subscriptions.billing._resolve_paid_save_card_payload",return_value=("",{})):
            billing.process_monobank_event(event)
        later.refresh_from_db()
        return later,event

    def assert_original_entitlement(self):
        self.claim.refresh_from_db(); self.payment.refresh_from_db()
        self.assertIsNotNone(self.claim.activated_at)
        sub=Subscription.objects.get(user=self.user)
        self.assertEqual(sub.payment_id,self.payment.provider_payment_id)
        self.assertEqual(self.payment.subscription_id,sub.pk)
        self.assertEqual(sub.trial_days,51)
        self.assertEqual(SubscriptionEvent.objects.filter(user=self.user,event_type="mono_trial_activated").count(),1)

    def test_old_paid_callback_cannot_restore_superseded_mandate(self):
        later,event=self.make_tokenless_later_paid()
        with patch("subscriptions.billing._resolve_paid_save_card_payload") as resolve:
            billing.process_monobank_event(self.event())
        resolve.assert_not_called()
        self.assertFalse(Subscription.objects.filter(user=self.user).exists())

    def test_newer_revoked_or_refunded_mandate_is_not_recovered(self):
        later,event=self.make_tokenless_later_paid()
        later.raw_payload["mandate_revoked_at"]="revoked"
        later.save()
        self.assertIsNone(billing.get_bind_activation_pending_payment(self.user.pk))
        with patch("subscriptions.billing.fetch_invoice_status") as fetch:
            billing.reconcile_pending_monobank_binds(limit=1)
        fetch.assert_not_called()
        later.status=Payment.Status.REFUNDED
        later.raw_payload.pop("mandate_revoked_at")
        later.save()
        self.assertIsNone(billing.get_bind_activation_pending_payment(self.user.pk))
        with patch("subscriptions.billing.fetch_invoice_status") as fetch:
            billing.reconcile_pending_monobank_binds(limit=1)
        fetch.assert_not_called()
        self.claim.refresh_from_db()
        self.assertIsNone(self.claim.activated_at)

    def test_newer_paid_tokenless_same_timestamp_recovers(self):
        later,event=self.make_tokenless_later_paid()
        with patch("subscriptions.billing._resolve_paid_save_card_payload",return_value=("late-token",{})):
            billing.process_monobank_event(event)
            billing.process_monobank_event(event)
        self.assert_original_entitlement()

    def test_newer_paid_tokenless_new_timestamp_recovers(self):
        later,event=self.make_tokenless_later_paid()
        event["modifiedDate"]="2026-11-14T09:00:00Z"
        with patch("subscriptions.billing._resolve_paid_save_card_payload",return_value=("late-token",{})):
            billing.process_monobank_event(event)
            billing.process_monobank_event(event)
        self.assert_original_entitlement()

    def test_newer_paid_tokenless_worker_and_checkout_guard(self):
        later,event=self.make_tokenless_later_paid()
        with patch("subscriptions.billing.create_invoice") as create:
            with self.assertRaisesRegex(ValueError,"^bind_activation_pending$"):
                billing.build_bind_invoice(user_id=self.user.pk,trial_days=51)
            create.assert_not_called()
        with patch("subscriptions.billing.fetch_invoice_status",return_value=event) as fetch, patch(
                "subscriptions.billing._resolve_paid_save_card_payload",return_value=("late-token",{})):
            billing.reconcile_pending_monobank_binds(limit=1)
        fetch.assert_called_once_with(later.provider_payment_id)
        self.assert_original_entitlement()

    def test_later_rebind_completes_original_partner_entitlement(self):
        with patch("subscriptions.billing._resolve_paid_save_card_payload", return_value=("", {})):
            billing.process_monobank_event(self.event())
        later=Payment.objects.create(user=self.user,provider="monobank",provider_payment_id="synthetic-rebind",
            amount="1.00",currency="UAH",kind=Payment.Kind.BIND,status=Payment.Status.PENDING,
            raw_payload={"bind_context":{"mode":"rebind","trial_granted":False}})
        event={**self.event(),"invoiceId":later.provider_payment_id}
        with patch("subscriptions.billing._resolve_paid_save_card_payload",return_value=("new-synthetic",{})):
            billing.process_monobank_event(event)
            billing.process_monobank_event(event)
        self.claim.refresh_from_db(); self.payment.refresh_from_db()
        self.assertIsNotNone(self.claim.activated_at)
        sub=Subscription.objects.get(user=self.user)
        self.assertEqual(sub.payment_id,self.payment.provider_payment_id)
        self.assertEqual(self.payment.subscription_id,sub.pk)
        self.assertEqual(sub.trial_days,51)
        self.assertEqual(SubscriptionEvent.objects.filter(event_type="mono_trial_activated").count(),1)
    def test_duplicate_refund_replay_preserves_manual_and_canonical(self):
        self.make_paid_pending()
        sub=Subscription.objects.create(user=self.user,status=Subscription.Status.MANUAL,
            source=Subscription.Source.ADMIN,expires_at=timezone.now()+timedelta(days=90))
        before=(sub.expires_at,sub.payment_id,sub.status,sub.source)
        duplicate=Payment.objects.create(user=self.user,provider="monobank",provider_payment_id="synthetic-duplicate",
            amount="1.00",currency="UAH",kind=Payment.Kind.BIND,status=Payment.Status.PAID,
            raw_payload={"bind_context":{"mode":"rebind","trial_granted":False},
                "refund_request":{"purpose":"duplicate_bind","status":"pending"}})
        event={**self.event(),"invoiceId":duplicate.provider_payment_id,"status":"refunded"}
        billing.process_monobank_event(event); billing.process_monobank_event(event)
        sub.refresh_from_db(); self.payment.refresh_from_db()
        self.assertEqual((sub.expires_at,sub.payment_id,sub.status,sub.source),before)
        self.assertEqual(self.payment.status,Payment.Status.PAID)
        self.assertIsNone(self.payment.subscription_id)
    def test_consumed_consent_never_creates_invoice(self):
        import json, uuid
        from django.test import RequestFactory
        from miniapp.views import billing_action
        from subscriptions.consent import accept_billing_consent, attach_consent_payment
        consent,_=accept_billing_consent(user=self.user,acquisition=None,idempotency_key=str(uuid.uuid4()),
            trial_days=51,copy_locale="uk")
        attach_consent_payment(consent=consent,payment=self.payment)
        request=RequestFactory().post("/",data=json.dumps({"action":"bind","consent_id":str(consent.pk)}),
            content_type="application/json")
        with patch("miniapp.views._get_session_user_or_response",return_value=self.user), patch(
                "miniapp.views.build_bind_invoice") as provider:
            response=billing_action(request)
        self.assertEqual(response.status_code,409)
        self.assertEqual(json.loads(response.content)["error"]["code"],"billing_consent_consumed")
        provider.assert_not_called()

    def test_worker_retries_are_bounded_and_diagnostics_are_token_free(self):
        self.make_paid_pending()
        with patch("subscriptions.billing.fetch_invoice_status",return_value=self.event()) as fetch, patch(
                "subscriptions.billing._resolve_paid_save_card_payload",return_value=("",{})):
            with self.assertLogs("subscriptions.billing",level="WARNING") as logs:
                billing.reconcile_pending_monobank_binds(limit=1)
            billing.reconcile_pending_monobank_binds(limit=1)
        self.assertEqual(fetch.call_count,1)
        self.payment.refresh_from_db()
        self.assertEqual(self.payment.raw_payload["activation_reconciliation"]["attempts"],1)
        self.assertIn("activation_pending",logs.output[0])
        self.assertNotIn("cardToken",logs.output[0])
    def test_active_profile_does_not_hide_missing_activation(self):
        from subscriptions.models import BillingProfile
        self.make_paid_pending()
        BillingProfile.objects.create(user=self.user,status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,card_token="synthetic")
        self.assertEqual(billing.get_bind_activation_pending_payment(self.user.pk).pk,self.payment.pk)

    def test_provider_failure_keeps_retry_budget_durable(self):
        self.make_paid_pending()
        with patch("subscriptions.billing.fetch_invoice_status",side_effect=billing.MonobankAPIError("synthetic")) as fetch:
            billing.reconcile_pending_monobank_binds(limit=1)
            billing.reconcile_pending_monobank_binds(limit=1)
        self.assertEqual(fetch.call_count,1)
        self.payment.refresh_from_db()
        self.assertEqual(self.payment.raw_payload["activation_reconciliation"]["attempts"],1)

    def test_exhausted_retry_budget_requires_operator_without_provider(self):
        self.make_paid_pending()
        self.payment.raw_payload["activation_reconciliation"]={"attempts":8}
        self.payment.save()
        with patch("subscriptions.billing.fetch_invoice_status") as provider:
            result=billing.sync_pending_bind_status(user_id=self.user.pk)
        provider.assert_not_called()
        self.assertTrue(result["operator_required"])
    def test_existing_trial_is_not_regranted(self):
        self.make_paid_pending()
        Subscription.objects.create(user=self.user,status=Subscription.Status.TRIAL,
            source=Subscription.Source.PAYMENT,expires_at=timezone.now()+timedelta(days=10))
        self.assertIsNone(billing.get_bind_activation_pending_payment(self.user.pk))

    def test_superseded_intent_and_legacy_bind_are_not_repairable(self):
        self.make_paid_pending()
        later=Payment.objects.create(user=self.user,provider="monobank",provider_payment_id="newer",
            amount="1.00",currency="UAH",kind=Payment.Kind.BIND,status=Payment.Status.PAID,
            raw_payload={"bind_context":{"trial_granted":False,"mode":"rebind"}})
        self.assertEqual(billing.get_bind_activation_pending_payment(self.user.pk).pk,self.payment.pk)
        later.delete()
        self.payment.raw_payload["bind_context"].pop("partner_claim_id")
        self.payment.save()
        self.assertIsNone(billing.get_bind_activation_pending_payment(self.user.pk))
    def test_worker_recovers_with_active_profile_without_legacy_replay(self):
        from subscriptions.models import BillingProfile
        self.make_paid_pending()
        BillingProfile.objects.create(user=self.user,status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,card_token="synthetic")
        with patch("subscriptions.billing.fetch_invoice_status",return_value=self.event()), patch(
                "subscriptions.billing._resolve_paid_save_card_payload",return_value=("synthetic",{})):
            billing.reconcile_pending_monobank_binds(limit=1)
            billing.reconcile_pending_monobank_binds(limit=1)
        self.claim.refresh_from_db()
        self.assertIsNotNone(self.claim.activated_at)
        self.assertEqual(SubscriptionEvent.objects.filter(event_type="mono_trial_activated").count(),1)

    def make_paid_pending(self):
        self.payment.status=Payment.Status.PAID
        self.payment.paid_at=timezone.now()
        self.payment.raw_payload["applied_effects"]={"paid":"recorded"}
        self.payment.save()
    def test_paid_pending_blocks_provider(self):
        self.make_paid_pending()
        with patch("subscriptions.billing.create_invoice") as provider:
            with self.assertRaisesRegex(ValueError,"^bind_activation_pending$"):
                billing.build_bind_invoice(user_id=self.user.pk,trial_days=51)
            provider.assert_not_called()
    def test_manual_grant_is_not_repaired(self):
        self.make_paid_pending()
        sub=Subscription.objects.create(user=self.user,status=Subscription.Status.MANUAL,
            source=Subscription.Source.ADMIN,expires_at=timezone.now()+timedelta(days=90))
        before=sub.expires_at
        self.assertIsNone(billing.get_bind_activation_pending_payment(self.user.pk))
        with patch("subscriptions.billing._resolve_paid_save_card_payload",return_value=("synthetic",{})):
            billing.process_monobank_event(self.event())
        sub.refresh_from_db(); self.claim.refresh_from_db()
        self.assertEqual(sub.expires_at,before)
        self.assertEqual(sub.status,Subscription.Status.MANUAL)
        self.assertIsNone(self.claim.activated_at)
    def test_refunded_and_revoked_are_not_pending(self):
        self.make_paid_pending()
        self.payment.raw_payload["mandate_revoked_at"]="revoked"; self.payment.save()
        self.assertIsNone(billing.get_bind_activation_pending_payment(self.user.pk))
        self.payment.raw_payload.pop("mandate_revoked_at")
        self.payment.status=Payment.Status.REFUNDED; self.payment.save()
        self.assertIsNone(billing.get_bind_activation_pending_payment(self.user.pk))
    def test_worker_recovers_paid_missing_activation(self):
        self.make_paid_pending()
        with patch("subscriptions.billing.fetch_invoice_status",return_value=self.event()), patch(
                "subscriptions.billing._resolve_paid_save_card_payload",return_value=("synthetic",{})):
            billing.reconcile_pending_monobank_binds(limit=1)
        self.claim.refresh_from_db()
        self.assertIsNotNone(self.claim.activated_at)

    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=84561001)
        link = PartnerLink.objects.create(name="Synthetic", code="lifecycle", trial_days=51)
        self.claim = PartnerTrialClaim.objects.create(user=self.user, partner_link=link,
            offer_id=f"partner_link:{link.pk}", code=link.code, name=link.name, trial_days=51)
        self.payment = Payment.objects.create(user=self.user, provider="monobank",
            provider_payment_id="synthetic-bind", amount="1.00", currency="UAH",
            kind=Payment.Kind.BIND, status=Payment.Status.PENDING,
            raw_payload={"bind_context":{"trial_days":51,"mode":"bind","trial_granted":True,
                "partner_claim_id":self.claim.pk}})
        for name in ("fetch_invoice_status", "fetch_wallet_cards"):
            mock = patch("subscriptions.billing."+name, return_value={})
            mock.start(); self.addCleanup(mock.stop)
    def event(self):
        return {"invoiceId":self.payment.provider_payment_id,"status":"success",
            "amount":100,"ccy":980,"modifiedDate":"2026-11-14T08:00:00Z"}
    def test_late_token_activates_partner_once(self):
        with patch("subscriptions.billing._resolve_paid_save_card_payload", return_value=("", {})):
            billing.process_monobank_event(self.event())
        self.assertFalse(Subscription.objects.filter(user=self.user).exists())
        with patch("subscriptions.billing._resolve_paid_save_card_payload", return_value=("synthetic", {})):
            billing.process_monobank_event(self.event())
            billing.process_monobank_event(self.event())
        self.claim.refresh_from_db()
        self.assertIsNotNone(self.claim.activated_at)
        sub = Subscription.objects.get(user=self.user)
        self.assertEqual(sub.trial_days,51)
        self.assertFalse(sub.auto_renew)
        self.assertEqual(SubscriptionEvent.objects.filter(user=self.user,event_type="mono_trial_activated").count(),1)
