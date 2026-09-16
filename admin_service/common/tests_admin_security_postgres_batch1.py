"""Opt-in ADMIN-001/002/003 PostgreSQL proofs in a disposable schema only."""
import json
from unittest import skipUnless
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType
from django.contrib.messages.storage.fallback import FallbackStorage
from django.core.exceptions import PermissionDenied
from django.test import RequestFactory, TestCase, override_settings
from django.utils import timezone

from audit_log.models import AdminAuditLog
from common import admin_pages
from common.admin_site import admin_site
from users.admin import TelegramUserAdmin
from users.models import TelegramUser, UserAdminState
from users.services import mark_user_as_test_user, reset_user_onboarding, unmark_user_as_test_user


@skipUnless(getattr(settings, 'ADMIN_SECURITY_SYNTHETIC_POSTGRES', False), 'Dedicated disposable PostgreSQL schema required')
@override_settings(ENABLE_ADMIN_TEST_TOOLS=True, ADMIN_TEST_TELEGRAM_IDS=[], MINIAPP_OPERATOR_TELEGRAM_IDS=[])
class AdminSecurityPostgresTests(TestCase):
    def setUp(self):
        # Content types are fixture-local and roll back between TestCase methods.
        ContentType.objects.clear_cache()
        self.actor = get_user_model().objects.create(username='qa', is_staff=True, is_active=True)
        self.actor.set_unusable_password()
        self.actor.save(update_fields=['password'])
        self.user = TelegramUser.objects.create(tg_user_id=101, first_name='Synthetic QA',
            onboarding_completed=True, onboarding_version=2, created_at=timezone.now())
        self.state = UserAdminState.objects.create(telegram_user=self.user,
            onboarding_payload={'accounts': [{'balance': '98765.43', 'name': 'PRIVATE-CANARY'}]},
            last_user_input='PRIVATE-CANARY salary 98765.43', last_bot_response='PRIVATE-CANARY balance')

    def grant(self, *pairs):
        group = Group.objects.create(name='role')
        for model, codename in pairs:
            ct = ContentType.objects.get_for_model(model)
            permission, _ = Permission.objects.get_or_create(content_type=ct, codename=codename, defaults={'name': codename})
            group.permissions.add(permission)
        self.actor.groups.add(group)
        self.actor = get_user_model().objects.get(pk=self.actor.pk)

    def request(self, method='post', **data):
        request = getattr(RequestFactory(), method)('/onboarding-debug/', data)
        request.user = self.actor
        request.session = {}
        request._messages = FallbackStorage(request)
        return request

    def test_staff_no_permissions_cannot_ban_or_test_mark(self):
        handler = TelegramUserAdmin(TelegramUser, admin_site)
        with self.assertRaises(PermissionDenied):
            handler.set_status_view(self.request(confirm='on'), '101', 'banned')
        with self.assertRaises(PermissionDenied):
            mark_user_as_test_user(user=self.user, admin_user=self.actor)
        self.state.refresh_from_db()
        self.assertEqual(self.state.status, 'active')
        self.assertFalse(self.state.is_test_user)
        self.assertFalse(AdminAuditLog.objects.exists())

    def test_real_user_manager_role_can_ban_with_confirmation_and_audit(self):
        self.grant((TelegramUser, 'change_telegramuser'))
        handler = TelegramUserAdmin(TelegramUser, admin_site)
        with patch.object(admin_site, 'each_context', return_value={}):
            response = handler.set_status_view(self.request('get'), '101', 'banned')
        self.assertEqual(response.status_code, 200)
        self.state.refresh_from_db()
        self.assertEqual(self.state.status, 'active')
        response = handler.set_status_view(self.request(confirm='on', comment='Synthetic RBAC proof'), '101', 'banned')
        self.assertEqual(response.status_code, 302)
        self.state.refresh_from_db()
        self.assertEqual(self.state.status, 'banned')
        self.assertEqual(AdminAuditLog.objects.get().action, 'user_banned')

    def test_test_marker_cannot_escalate_to_cleanup_and_audit_is_allowlisted(self):
        self.grant((UserAdminState, 'can_use_test_tools'), (UserAdminState, 'change_useradminstate'))
        mark_user_as_test_user(user=self.user, admin_user=self.actor, notes='Synthetic test target')
        self.state.refresh_from_db()
        self.assertTrue(self.state.is_test_user)
        with self.assertRaises(PermissionDenied):
            reset_user_onboarding(101, 'cleanup', self.actor, 'QA', {'double_confirmed': True, 'dry_run': True})
        unmark_user_as_test_user(user=self.user, admin_user=self.actor)
        records = list(AdminAuditLog.objects.values('before', 'after'))
        self.assertEqual(len(records), 2)
        encoded = json.dumps(records)
        self.assertNotIn('PRIVATE-CANARY', encoded)
        self.assertNotIn('98765.43', encoded)
        for record in records:
            self.assertEqual(set(record['before']), {'is_test_user', 'marked_as_test_by_id', 'marked_as_test_at'})
        self.state.refresh_from_db()
        self.assertIn('PRIVATE-CANARY', self.state.last_user_input)
        self.assertFalse(self.state.is_test_user)

    def test_safe_reset_receipt_and_persisted_audit_exclude_financial_canary(self):
        self.grant((TelegramUser, 'change_telegramuser'))
        result = reset_user_onboarding(101, 'safe', self.actor, 'Synthetic safe reset', {
            'send_telegram_notice': False, 'send_admin_notification': False})
        audit = AdminAuditLog.objects.get(action='onboarding_reset')
        encoded = json.dumps({'result': result, 'before': audit.before, 'after': audit.after})
        self.assertNotIn('PRIVATE-CANARY', encoded)
        self.assertNotIn('98765.43', encoded)
        self.assertEqual(result['before']['user']['onboarding_completed'], True)
        self.assertEqual(result['after']['user']['onboarding_completed'], False)
        self.state.refresh_from_db()
        self.assertEqual(self.state.last_user_input, '')
        self.assertEqual(self.state.onboarding_payload, {})

    def test_debug_html_requires_test_target_reason_and_persisted_read_audit(self):
        self.grant((UserAdminState, 'can_use_test_tools'))
        with patch.object(admin_site, 'each_context', return_value={}):
            response = admin_pages.onboarding_debug_view(admin_site, self.request(user_lookup='101', reason='QA'))
            self.assertIsNone(response.context_data['debug'])
            self.assertFalse(AdminAuditLog.objects.exists())
            self.state.is_test_user = True
            self.state.save(update_fields=['is_test_user'])
            for request in (self.request('get', user_id='101'), self.request(user_lookup='101', reason='')):
                response = admin_pages.onboarding_debug_view(admin_site, request)
                self.assertIsNone(response.context_data['debug'])
                response.render()
                self.assertNotIn(b'PRIVATE-CANARY', response.content)
            self.assertFalse(AdminAuditLog.objects.exists())
            response = admin_pages.onboarding_debug_view(admin_site, self.request(user_lookup='101', reason='QA fixture diagnosis'))
            audit = AdminAuditLog.objects.get(action='onboarding_debug_viewed')
            self.assertEqual(audit.reason, 'QA fixture diagnosis')
            self.assertEqual(audit.target_user_id, 101)
            self.assertNotIn('PRIVATE-CANARY', json.dumps({'before': audit.before, 'after': audit.after}))
            response.render()
        self.assertIn(b'PRIVATE-CANARY', response.content)
        self.assertIn(b'name="reason"', response.content)
