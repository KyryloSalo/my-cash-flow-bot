"""Synthetic PostgreSQL backend regressions; run with mini_backend_batch2_runner.py."""
from __future__ import annotations

import copy
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

from django.conf import settings
from django.db import connection, connections
from django.test import Client, RequestFactory
from django.utils import timezone

from accounts.models import Account
from categories.models import Category, CategoryTemplate
from subscriptions.models import BillingProfile, Subscription
from transactions.models import Debt, DebtPayment, Transaction
from users.models import TelegramUser, UserAdminState
from miniapp import auth, family, onboarding, services, views
from miniapp.models import DraftAction


@unittest.skipUnless(getattr(settings, "MINI_BACKEND_BATCH2_TESTS", False), "requires isolated batch2 schema")
class BackendBatch2Tests(unittest.TestCase):
    def setUp(self):
        with connection.cursor() as cursor:
            cursor.execute('SELECT current_schema()')
            self.assertTrue(cursor.fetchone()[0].startswith('mini_backend2_'))
            cursor.execute('TRUNCATE users, accounts, categories, category_templates, transactions, debts, debt_payments, families, family_members, family_invites, miniapp_draft_actions, miniapp_write_receipts, django_session RESTART IDENTITY CASCADE')
        self.user = self.make_user(950001)
        self.account = self.make_account(self.user, 'Cash', 'UAH', '1000')

    def make_user(self, pk):
        return TelegramUser.objects.create(tg_user_id=pk, lang='en', base_currency='UAH', created_at=timezone.now())

    def make_account(self, user, label, currency='UAH', balance='0', family_id=None):
        return Account.objects.create(tg_user=user, family_id=family_id, label=label, currency=currency, balance=Decimal(balance), starting_balance=Decimal(balance), account_type='main', non_negative_account_type='main', is_active=True, created_at=timezone.now(), updated_at=timezone.now())

    def grant(self, user, status='active', days=1, scope='personal_full', source='billing'):
        UserAdminState.objects.update_or_create(telegram_user=user, defaults={'access_scope': scope, 'access_source': source})
        return Subscription.objects.create(user=user, status=status, expires_at=timezone.now() + timedelta(days=days) if days is not None else None)

    def client_for(self, user):
        client = Client()
        session = client.session
        session[auth.SESSION_USER_ID_KEY] = user.tg_user_id
        session.save()
        return client

    def post(self, client, path, payload):
        return client.post('/app/api/' + path, data=json.dumps(payload), content_type='application/json')

    def assert_domain(self, fn, code=None, status=None):
        with self.assertRaises(services.MiniAppTransactionError) as caught:
            fn()
        if code is not None:
            self.assertEqual(caught.exception.code, code)
        if status is not None:
            self.assertEqual(caught.exception.status, status)

    def test_mini13_nonfinite_and_oversized_money_returns_400_without_draft(self):
        self.grant(self.user)
        client = self.client_for(self.user)
        for raw in ('NaN', 'sNaN', 'Infinity', '-Infinity', '1e100', '10000000000000000', '9999999999999999.995'):
            with self.subTest(raw=raw):
                response = self.post(client, 'accounts/draft', {'action': 'create', 'label': 'Invalid', 'currency': 'UAH', 'starting_balance': raw})
                self.assertEqual(response.status_code, 400, response.content)
                self.assertEqual(DraftAction.objects.count(), 0)
                self.assertEqual(Account.objects.count(), 1)

    def test_mini13_amount_and_rate_helpers_reject_nonfinite_bounds(self):
        for parser in (services._parse_transaction_amount, services._parse_signed_transaction_amount, services._parse_optional_rate):
            for raw in ('NaN', 'sNaN', 'Infinity', '-Infinity', '1e100', '1e999999999999999999', '1' * 200):
                with self.subTest(parser=parser.__name__, raw=raw):
                    self.assert_domain(lambda: parser(raw))
        self.assertEqual(services._parse_transaction_amount('1,25'), Decimal('1.25'))
        self.assertEqual(services._parse_signed_transaction_amount('-1.25'), Decimal('-1.25'))

    def test_mini13_transfer_overflow_underflow_and_balance_bounds(self):
        target = self.make_account(self.user, 'USD', 'USD')
        for amount, rate in (('9999999999999999.99', '0.0001'), ('0.01', '10000')):
            with self.subTest(amount=amount, rate=rate):
                self.assert_domain(lambda: services.build_transfer_draft(self.user, {'source_account_id': self.account.id, 'target_account_id': target.id, 'amount': amount, 'fx_rate': rate}, draft_id='overflow'))
        maximum = '9999999999999999.99'
        draft = services.build_account_draft(self.user, {'action':'create', 'label':'Boundary', 'currency':'UAH', 'starting_balance':maximum}, draft_id='boundary')
        result = services.commit_account_draft(self.user, draft)
        self.assertEqual(Account.objects.get(id=result['account']['id']).balance, Decimal(maximum))
        Account.objects.filter(id=target.id).update(balance=Decimal(maximum), currency='UAH')
        self.assert_domain(lambda: services.build_transfer_draft(self.user, {'source_account_id':self.account.id, 'target_account_id':target.id, 'amount':'1'}, draft_id='sum-overflow'))

    def make_debt(self, direction='payable', currency='UAH', paid='0'):
        return Debt.objects.create(tg_user=self.user, counterparty_name='Synthetic debt', direction=direction, initial_amount=Decimal('100'), paid_amount=Decimal(paid), remaining_amount=Decimal('100')-Decimal(paid), currency=currency, status='partially_paid' if Decimal(paid) else 'active', created_at=timezone.now(), updated_at=timezone.now())

    def repayment(self, debt, amount='80', account_amount=None):
        return services.build_debt_draft(self.user, {'action':'repay', 'debt_id':debt.id, 'amount':amount, 'account_id':self.account.id, 'account_amount':account_amount}, draft_id='repay')

    def test_mini04_two_reviewed_repayments_never_cap_only_principal(self):
        for direction in ('payable', 'receivable'):
            for currency, account_amount in (('UAH', None), ('USD', '320')):
                with self.subTest(direction=direction, currency=currency):
                    debt = self.make_debt(direction, currency)
                    first = self.repayment(debt, account_amount=account_amount)
                    stale = copy.deepcopy(first)
                    services.commit_debt_draft(self.user, first)
                    before = (DebtPayment.objects.count(), Transaction.objects.count(), Account.objects.get(id=self.account.id).balance)
                    self.assert_domain(lambda: services.commit_debt_draft(self.user, stale), 'debt_changed', 409)
                    self.assertEqual(before, (DebtPayment.objects.count(), Transaction.objects.count(), Account.objects.get(id=self.account.id).balance))
                    debt.refresh_from_db()
                    self.assertEqual(debt.remaining_amount, Decimal('20'))
                    fresh = self.repayment(debt, '20', '80' if currency == 'USD' else None)
                    services.commit_debt_draft(self.user, fresh)
                    debt.refresh_from_db()
                    self.assertEqual(debt.remaining_amount, Decimal('0'))

    def test_mini04_debt_and_account_currency_drift_rejects_before_writes(self):
        for changes in ({'currency':'USD'}, {'direction':'receivable'}, {'remaining_amount':Decimal('90')}, {'status':'closed'}):
            debt = self.make_debt()
            draft = self.repayment(debt)
            Debt.objects.filter(id=debt.id).update(**changes)
            self.assert_domain(lambda: services.commit_debt_draft(self.user, draft), status=409)
        debt = self.make_debt()
        draft = self.repayment(debt)
        Account.objects.filter(id=self.account.id).update(currency='USD')
        self.assert_domain(lambda: services.commit_debt_draft(self.user, draft), status=409)
        self.assertEqual(DebtPayment.objects.count(), 0)
        self.assertEqual(Transaction.objects.count(), 0)

    def test_transfer_commits_only_reviewed_currency_pair_and_target_amount(self):
        target = self.make_account(self.user, 'Target', 'USD')
        payload = {'source_account_id':self.account.id, 'target_account_id':target.id, 'amount':'80', 'fx_rate':'40'}
        for change in ('source_currency', 'target_currency', 'target_amount'):
            Account.objects.filter(id=self.account.id).update(currency='UAH')
            Account.objects.filter(id=target.id).update(currency='USD')
            draft = services.build_transfer_draft(self.user, payload, draft_id='transfer')
            if change == 'source_currency':
                Account.objects.filter(id=self.account.id).update(currency='EUR')
            elif change == 'target_currency':
                Account.objects.filter(id=target.id).update(currency='EUR')
            else:
                draft['target_amount_value'] = '4.00'
            self.assert_domain(lambda: services.commit_transfer_draft(self.user, draft), 'transfer_changed', 409)
        self.assertEqual(Transaction.objects.count(), 0)
        self.assertEqual(Account.objects.get(id=self.account.id).balance, Decimal('1000'))

    def test_fin005_debt_debits_respect_explicit_limit_under_account_lock(self):
        for action in ('create', 'repay'):
            for limit in ('0', '50'):
                Account.objects.filter(id=self.account.id).update(balance=Decimal('1000'), credit_limit=None)
                if action == 'create':
                    draft = services.build_debt_draft(self.user, {'action':'create', 'direction':'receivable', 'counterparty_name':'Borrower', 'amount':'100', 'currency':'UAH', 'account_id':self.account.id}, draft_id='debit')
                else:
                    draft = self.repayment(self.make_debt(), '100')
                Account.objects.filter(id=self.account.id).update(balance=Decimal('0'), credit_limit=Decimal(limit))
                before = (Debt.objects.count(), DebtPayment.objects.count(), Transaction.objects.count())
                self.assert_domain(lambda: services.commit_debt_draft(self.user, draft), 'credit_limit_exceeded')
                self.assertEqual(before, (Debt.objects.count(), DebtPayment.objects.count(), Transaction.objects.count()))
                self.assertEqual(Account.objects.get(id=self.account.id).balance, Decimal('0'))

    def test_mini13_debt_projected_balance_overflow_is_domain_error(self):
        debt = self.make_debt('receivable')
        draft = self.repayment(debt)
        Account.objects.filter(id=self.account.id).update(balance=Decimal('9999999999999999.99'))
        self.assert_domain(lambda: services.commit_debt_draft(self.user, draft))
        self.assertEqual(DebtPayment.objects.count(), 0)

    def test_mini08_currency_edit_forbids_paid_or_recorded_debts(self):
        debt = self.make_debt(currency='USD', paid='20')
        self.assert_domain(lambda: services.build_debt_draft(self.user, {'action':'edit', 'debt_id':debt.id, 'currency':'UAH', 'amount':'4000'}, draft_id='edit'), 'debt_currency_locked')
        clean = self.make_debt(currency='USD')
        draft = services.build_debt_draft(self.user, {'action':'edit', 'debt_id':clean.id, 'currency':'UAH', 'amount':'4000'}, draft_id='edit')
        # History is authoritative even if a legacy paid projection is zero.
        DebtPayment.objects.create(tg_user=self.user, debt=clean, amount=Decimal('20'), currency='USD', account=self.account, payment_date=timezone.localdate(), created_at=timezone.now())
        self.assert_domain(lambda: services.commit_debt_draft(self.user, draft), 'debt_currency_locked')
        clean.refresh_from_db()
        self.assertEqual(clean.currency, 'USD')
        DebtPayment.objects.all().delete()
        Transaction.objects.create(tg_user=self.user, debt=clean, type='debt_received', flow_kind='debt', amount=Decimal('100'), currency='USD', source='synthetic', date=timezone.localdate(), created_at=timezone.now())
        self.assert_domain(lambda: services.commit_debt_draft(self.user, draft), 'debt_currency_locked')

    def test_mini04_overpay_preview_requires_new_amount_not_silent_cap(self):
        debt = self.make_debt()
        self.assert_domain(lambda: self.repayment(debt, '120'), 'debt_amount_exceeded')

    def test_fin005_incoming_can_reduce_existing_over_limit_debt(self):
        Account.objects.filter(id=self.account.id).update(balance=Decimal('-200'), credit_limit=Decimal('50'), account_type='credit', non_negative_account_type='savings')
        debt = self.make_debt('receivable')
        services.commit_debt_draft(self.user, self.repayment(debt, '100'))
        self.assertEqual(Account.objects.get(id=self.account.id).balance, Decimal('-100'))

    def test_mini13_debt_preview_rejects_overflow_and_derived_fx_rate(self):
        Account.objects.filter(id=self.account.id).update(balance=Decimal('9999999999999999.99'))
        self.assert_domain(lambda: services.build_debt_draft(self.user, {'action':'create','direction':'payable','counterparty_name':'Synthetic','amount':'1','currency':'UAH','account_id':self.account.id}, draft_id='overflow'))
        self.assert_domain(lambda: services._resolve_debt_account_amount(debt_amount=Decimal('0.01'), debt_currency='USD', account_currency='UAH', account_amount_raw='9999999999999999.99', fx_rate_raw=None))

    def make_family(self, owner, member=None):
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO families (name,owner_user_id,status,created_at,updated_at) VALUES ('Synthetic',%s,'active',now(),now()) RETURNING id", [owner.tg_user_id])
            family_id = cursor.fetchone()[0]
            for user, role in ((owner, 'owner'), (member, 'member')):
                if user is not None:
                    cursor.execute("INSERT INTO family_members (family_id,user_id,role,status,joined_at,created_at,updated_at) VALUES (%s,%s,%s,'active',now(),now(),now())", [family_id,user.tg_user_id,role])
        return family_id

    def test_bill001_expired_projection_never_grants_current_access(self):
        for status, source in (('active','billing'), ('trial','billing'), ('manual','admin_manual'), ('expired','billing'), ('cancelled','billing')):
            subscription = self.grant(self.user, status=status, days=-1, source=source)
            self.assertEqual(services.resolve_access(self.user)['mode'], 'blocked', (status, source))
            response = self.post(self.client_for(self.user), 'accounts/draft', {'action':'create','label':'Blocked','currency':'UAH'})
            self.assertEqual(response.status_code, 403)
        with connection.cursor() as cursor:
            cursor.execute('DELETE FROM subscriptions WHERE telegram_user_id=%s', [self.user.tg_user_id])
        self.assertEqual(services.resolve_access(self.user)['mode'], 'blocked')
        self.grant(self.user, status='manual', days=None, source='admin_manual')
        self.assertEqual(services.resolve_access(self.user)['mode'], 'active')

    def test_bill001_soft_grace_is_full_then_read_only_or_blocked(self):
        sub = self.grant(self.user, status='expired', days=-1)
        sub.grace_expires_at = timezone.now()+timedelta(hours=1)
        sub.save()
        profile = BillingProfile.objects.create(user=self.user, wallet_id='synthetic', last_failure_reason='insufficient funds')
        self.assertEqual(services.resolve_access(self.user)['mode'], 'active')
        profile.last_failure_reason = 'technical error'
        profile.save()
        self.assertEqual(services.resolve_access(self.user)['mode'], 'grace_read_only')
        sub.grace_expires_at = timezone.now()-timedelta(seconds=1)
        sub.save()
        self.assertEqual(services.resolve_access(self.user)['mode'], 'blocked')

    def test_mini02_family_access_uses_current_membership_and_sponsor(self):
        owner = self.make_user(950002)
        sub = self.grant(owner)
        UserAdminState.objects.create(telegram_user=self.user, access_scope='family_full', access_source='family')
        self.assertEqual(services.resolve_access(self.user)['mode'], 'blocked')
        family_id = self.make_family(owner, self.user)
        UserAdminState.objects.filter(telegram_user=self.user).update(access_scope='paywall')
        self.assertEqual(services.resolve_access(self.user)['mode'], 'active')
        sub.expires_at = timezone.now()-timedelta(seconds=1)
        sub.save()
        UserAdminState.objects.filter(telegram_user=self.user).update(access_scope='family_full')
        self.assertEqual(services.resolve_access(self.user)['mode'], 'blocked')
        sub.expires_at = timezone.now()+timedelta(days=1)
        sub.save()
        family.remove_family_member(owner, member_user_id=self.user.tg_user_id)
        self.assertEqual(services.resolve_access(self.user)['mode'], 'blocked')
        self.assertFalse(services._resolve_finance_scope(self.user).is_family)
        self.assertNotEqual(UserAdminState.objects.get(telegram_user=self.user).access_scope, 'family_full')

    def test_mini02_owner_requires_own_entitlement_and_bans_win(self):
        self.make_family(self.user)
        UserAdminState.objects.create(telegram_user=self.user, access_scope='family_full')
        self.assertEqual(services.resolve_access(self.user)['mode'], 'blocked')
        self.grant(self.user)
        UserAdminState.objects.filter(telegram_user=self.user).update(status='banned', is_blocked=False)
        self.assertEqual(services.resolve_access(self.user)['mode'], 'blocked')

    def parallel(self, calls):
        barrier = threading.Barrier(len(calls))
        def invoke(fn):
            conn = connections['default']
            try:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT pg_backend_pid()')
                    pid = cursor.fetchone()[0]
                barrier.wait(timeout=4)
                try:
                    return 'ok', fn(), pid
                except (family.MiniAppFamilyError, services.MiniAppTransactionError) as exc:
                    return exc.code, exc.status, pid
            finally:
                conn.close()
        with ThreadPoolExecutor(max_workers=len(calls)) as pool:
            results = list(pool.map(invoke, calls))
        self.assertEqual(len({r[2] for r in results}), len(calls))
        return results

    def test_mini10_family_creations_serialize_absent_membership(self):
        from django.db import transaction
        entered = threading.Event()
        ready = threading.Event()
        original = family._active_membership
        def observe(*args, **kwargs):
            entered.set()
            return original(*args, **kwargs)
        def create():
            try:
                user = TelegramUser.objects.get(pk=self.user.pk)
                ready.set()
                return family.create_family(user, name='Synthetic')
            finally:
                connections['default'].close()
        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                TelegramUser.objects.select_for_update().get(pk=self.user.pk)
                with patch.object(family, '_active_membership', side_effect=observe):
                    future = pool.submit(create)
                    self.assertTrue(ready.wait(4), 'worker never reached the service')
                    self.assertFalse(entered.wait(0.5), 'membership check ran without the actor lock')
            future.result(timeout=8)
        results = self.parallel([lambda: family.create_family(TelegramUser.objects.get(pk=self.user.pk), name='Second'), lambda: family.create_family(TelegramUser.objects.get(pk=self.user.pk), name='Third')])
        self.assertEqual([r[0] for r in results], ['already_in_family', 'already_in_family'])
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM family_members WHERE user_id=%s AND status='active'", [self.user.pk])
            self.assertEqual(cursor.fetchone()[0], 1)

    def test_mini12_account_create_guard_waits_for_shared_scope_lock(self):
        from django.db import transaction
        owner = self.make_user(950002)
        fid = self.make_family(owner, self.user)
        first = services.build_account_draft(self.user, {'action':'create', 'label':'Shared','currency':'UAH'}, draft_id='first')
        second = services.build_account_draft(owner, {'action':'create', 'label':'sHARED','currency':'UAH'}, draft_id='second')
        entered = threading.Event()
        ready = threading.Event()
        original = services._ensure_unique_account_label
        def observe(*args, **kwargs):
            entered.set()
            return original(*args, **kwargs)
        def commit():
            try:
                user = TelegramUser.objects.get(pk=self.user.pk)
                ready.set()
                return services.commit_account_draft(user, first)
            finally:
                connections['default'].close()
        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute('SELECT id FROM families WHERE id=%s FOR UPDATE', [fid])
                with patch.object(services, '_ensure_unique_account_label', side_effect=observe):
                    future = pool.submit(commit)
                    self.assertTrue(ready.wait(4), 'worker never reached the service')
                    self.assertFalse(entered.wait(0.5), 'label guard ran without shared family lock')
            future.result(timeout=8)
        self.assert_domain(lambda: services.commit_account_draft(owner, second), 'account_label_exists')

    def test_mini11_paywall_invitee_can_join_entitled_family_without_own_subscription(self):
        owner = self.make_user(950002)
        self.grant(owner)
        fid = self.make_family(owner)
        token = family.create_family_invite(owner)['invite']['token']
        with patch.object(views, 'emit_notification'):
            response = self.post(self.client_for(self.user), 'family/invite/accept', {'token':token})
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(services.resolve_access(self.user)['mode'], 'active')
        self.assertEqual(services._resolve_finance_scope(self.user).family_id, fid)
        self.assertEqual(UserAdminState.objects.get(telegram_user=self.user).access_scope, 'family_full')

    def test_mini11_invalid_sponsor_and_ban_do_not_consume_invite(self):
        owner = self.make_user(950002)
        self.make_family(owner)
        invite = family.create_family_invite(owner)['invite']
        with self.assertRaises(family.MiniAppFamilyError) as caught:
            family.accept_family_invite(self.user, token=invite['token'])
        self.assertEqual(caught.exception.code, 'family_sponsor_inactive')
        self.grant(owner)
        UserAdminState.objects.create(telegram_user=self.user, status='banned')
        with patch.object(views, 'emit_notification'):
            response = self.post(self.client_for(self.user), 'family/invite/accept', {'token':invite['token']})
        self.assertEqual(response.status_code, 403)
        with connection.cursor() as cursor:
            cursor.execute('SELECT used_count FROM family_invites WHERE id=%s', [invite['id']])
            self.assertEqual(cursor.fetchone()[0], 0)

    def test_mini10_join_join_and_create_join_have_one_membership(self):
        owners = [self.make_user(950010+i) for i in range(2)]
        for owner in owners:
            self.grant(owner)
            self.make_family(owner)
        tokens = [family.create_family_invite(owner)['invite']['token'] for owner in owners]
        results = self.parallel([lambda token=token: family.accept_family_invite(TelegramUser.objects.get(pk=self.user.pk), token=token) for token in tokens])
        self.assertEqual(sorted(r[0] for r in results), ['already_in_family', 'ok'])
        with connection.cursor() as cursor:
            cursor.execute('SELECT sum(used_count) FROM family_invites')
            self.assertEqual(cursor.fetchone()[0], 1)
        other = self.make_user(950003)
        fresh = family.create_family_invite(owners[0])['invite']['token']
        results = self.parallel([lambda: family.create_family(TelegramUser.objects.get(pk=other.pk), name='New'), lambda: family.accept_family_invite(TelegramUser.objects.get(pk=other.pk), token=fresh)])
        self.assertEqual(sorted(r[0] for r in results), ['already_in_family', 'ok'])

    def make_templates(self):
        for kind in ('income', 'expense'):
            CategoryTemplate.objects.create(type=kind, name=kind.title(), slug=kind, aliases=[], sort_order=1, is_active=True, created_at=timezone.now(), updated_at=timezone.now())

    def test_mini09_family_repair_reuses_migrated_defaults_with_legacy_unique_index(self):
        from django.db import transaction
        self.make_templates()
        with transaction.atomic():
            onboarding._ensure_default_categories(self.user)
        family.create_family(self.user, name='Migrated')
        self.user.__dict__.pop('_miniapp_finance_scope', None)
        draft = onboarding.build_onboarding_draft(self.user, {'accounts':[], 'start_date':'2026-09-01', 'base_currency':'UAH'}, draft_id='repair')
        result = onboarding.commit_onboarding_draft(self.user, draft)
        self.assertTrue(result['onboarding']['completed'])
        self.assertEqual(Category.objects.count(), 2)
        self.assertEqual(set(services._scoped_categories_qs(self.user).values_list('type', flat=True)), {'income','expense'})
        self.assertFalse(Category.objects.filter(family_id__isnull=True).exists())

    def test_mini09_empty_family_defaults_are_scoped_even_with_personal_duplicates(self):
        from django.db import transaction
        self.make_templates()
        with transaction.atomic():
            onboarding._ensure_default_categories(self.user)
        owner = self.make_user(950002)
        fid = self.make_family(owner, self.user)
        self.user.__dict__.pop('_miniapp_finance_scope', None)
        draft = onboarding.build_onboarding_draft(self.user, {'accounts':[{'label':'Family cash','currency':'UAH'}], 'start_date':'2026-09-01'}, draft_id='setup')
        result = onboarding.commit_onboarding_draft(self.user, draft)
        self.assertTrue(result['onboarding']['completed'])
        self.assertEqual(set(Category.objects.filter(family_id=fid, is_active=True).values_list('type', flat=True)), {'income','expense'})
        self.assertEqual(Account.objects.get(label='Family cash').family_id, fid)
        Category.objects.filter(family_id=fid, type='expense').update(is_active=False)
        self.user.refresh_from_db()
        self.assertFalse(onboarding.onboarding_is_complete(self.user))

    def test_mini07_logout_and_identity_switch_clear_private_session_namespace(self):
        from django.test import override_settings
        from types import SimpleNamespace
        other = self.make_user(950002)
        self.grant(self.user)
        self.grant(other)
        for mode, logout in (('telegram', False), ('telegram', True), ('browser', False), ('browser', True)):
            with self.subTest(mode=mode, logout=logout):
                client = self.client_for(self.user)
                response = self.post(client, 'accounts/draft', {'action':'create','label':'A private','currency':'UAH'})
                self.assertEqual(response.status_code, 200)
                draft_id = response.json()['draft']['draft_id']
                session = client.session
                session[views.AI_IMAGE_BATCH_SESSION_KEY] = {'batch_id':'private','items':[{'id':'row','suggestion':{'comment':'A private statement'}}]}
                session['unrelated_admin_state'] = 'preserve'
                old_key = session.session_key
                session.save()
                if logout:
                    self.assertEqual(self.post(client,'auth/logout',{}).status_code,200)
                    self.assertFalse(any(key.startswith('miniapp_') for key in client.session.keys()))
                if mode == 'telegram':
                    with patch.object(views, 'validate_telegram_init_data', return_value=SimpleNamespace(tg_user_id=other.pk,first_name='B',raw_user={})):
                        response = self.post(client, 'auth/telegram', {'init_data':'synthetic validated identity'})
                    self.assertEqual(response.status_code,200)
                else:
                    with override_settings(MINIAPP_BROWSER_LOGIN_SECRET='synthetic-only-key'):
                        token = auth.build_browser_login_token(other.pk)
                        response = client.get('/app/api/browser-login/'+token+'/')
                    self.assertEqual(response.status_code,302)
                self.assertNotEqual(client.session.session_key,old_key)
                self.assertEqual(client.session['unrelated_admin_state'],'preserve')
                self.assertNotIn(views.AI_IMAGE_BATCH_SESSION_KEY,client.session)
                self.assertNotIn(views.ACCOUNT_DRAFTS_SESSION_KEY,client.session)
                self.assertEqual(client.get('/app/api/ai-image/batch').json()['batch'],None)
                self.assertIn(self.post(client,'accounts/confirm',{'draft_id':draft_id,'idempotency_key':'switch-identity-key00001'}).status_code, (404,409))

    def test_mini07_replayed_batch_and_changed_scope_cannot_reveal_or_apply_intent(self):
        self.grant(self.user)
        client = self.client_for(self.user)
        response = self.post(client,'accounts/draft',{'action':'create','label':'Personal only','currency':'UAH'})
        draft_id = response.json()['draft']['draft_id']
        session = client.session
        session[views.AI_IMAGE_BATCH_SESSION_KEY] = {'batch_id':'other-actor','actor_user_id':950999,'items':[{'id':'private','suggestion':{'comment':'private'}}]}
        session.save()
        self.assertEqual(client.get('/app/api/ai-image/batch').json()['batch'],None)
        self.make_family(self.user)
        response = self.post(client,'accounts/confirm',{'draft_id':draft_id,'idempotency_key':'scope-change-key-00001'})
        self.assertEqual(response.status_code,409,response.content)
        self.assertFalse(Account.objects.filter(label='Personal only').exists())

    def test_mini07_changed_scope_rejects_personal_draft(self):
        self.grant(self.user)
        client = self.client_for(self.user)
        response = self.post(client,'accounts/draft',{'action':'create','label':'Personal only','currency':'UAH'})
        draft_id = response.json()['draft']['draft_id']
        self.make_family(self.user)
        response = self.post(client,'accounts/confirm',{'draft_id':draft_id,'idempotency_key':'scope-change-key-00001'})
        self.assertEqual(response.status_code,409,response.content)
        self.assertFalse(Account.objects.filter(label='Personal only').exists())