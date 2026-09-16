"""ADMIN-001/002/003: real admin handlers, synthetic principals, no DB or sends."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import override_settings

from broadcasts.admin import BroadcastAdmin
from broadcasts.models import Broadcast
from common import admin_pages
from polls.admin import PollCampaignAdmin
from polls.models import PollCampaign
from subscriptions.admin import BillingProfileAdmin, TrialRecoveryCampaignAdmin, confirm_manually, reject_payment, mark_refunded
from subscriptions.models import BillingProfile, TrialRecoveryCampaign
from support.admin import SupportCaseAdmin
from support.models import SupportCase
from users import services
from users.models import UserAdminState

from django.core.exceptions import PermissionDenied
from django.test import RequestFactory, SimpleTestCase

from common.admin_site import admin_site
from users.admin import TelegramUserAdmin
from users.models import TelegramUser


def principal(*permissions, superuser=False, staff=True, active=True):
    return SimpleNamespace(pk=77, is_authenticated=True, is_active=active, is_staff=staff,
                           is_superuser=superuser, has_perm=lambda p: p in permissions)


class LookupReached(Exception):
    """Read boundary sentinel; never allows the action to reach persistence."""


class AdminRoutePermissionTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.model_admin = TelegramUserAdmin(TelegramUser, admin_site)

    def test_staff_without_permissions_cannot_ban_before_lookup(self):
        request = self.factory.post('/admin/users/telegramuser/101/status/banned/', {'confirm': 'on'})
        request.user = principal()
        with patch('users.admin.get_object_or_404', side_effect=LookupReached) as lookup:
            try:
                self.model_admin.set_status_view(request, '101', 'banned')
            except Exception as exc:
                self.assertIsInstance(exc, PermissionDenied)
            else:
                self.fail('Denied action returned normally')
        lookup.assert_not_called()

    def test_user_manager_can_reach_status_confirmation(self):
        request = self.factory.get('/admin/users/telegramuser/101/status/banned/')
        request.user = principal('users.change_telegramuser')
        with patch('users.admin.get_object_or_404', side_effect=LookupReached):
            with self.assertRaises(LookupReached):
                self.model_admin.set_status_view(request, '101', 'banned')


def custom_route_cases():
    users = TelegramUserAdmin(TelegramUser, admin_site)
    broadcasts = BroadcastAdmin(Broadcast, admin_site)
    polls = PollCampaignAdmin(PollCampaign, admin_site)
    support = SupportCaseAdmin(SupportCase, admin_site)
    billing = BillingProfileAdmin(BillingProfile, admin_site)
    recovery = TrialRecoveryCampaignAdmin(TrialRecoveryCampaign, admin_site)
    yield users.set_status_view, ('101', 'banned'), 'users.admin.get_object_or_404', ('users.change_telegramuser',)
    for method, permission in (
        ('send_message_view', ('broadcasts.add_adminmessagelog',)),
        ('force_charge_now_view', ('subscriptions.change_payment',)),
        ('refund_latest_payment_view', ('subscriptions.change_payment',)),
        ('reset_onboarding_view', ('users.change_telegramuser',)),
        ('erase_user_data_view', ('users.can_use_test_tools', 'users.delete_telegramuser')),
        ('hard_delete_user_view', ('users.can_use_test_tools', 'users.delete_telegramuser')),
        ('add_note_view', ('users.add_adminnote',)),
        ('mark_test_user_view', ('users.can_use_test_tools', 'users.change_useradminstate')),
        ('unmark_test_user_view', ('users.can_use_test_tools', 'users.change_useradminstate')),
    ):
        yield getattr(users, method), ('101',), 'users.admin.get_object_or_404', permission
    yield users.subscription_action_view, ('101', 'trial'), 'users.admin.get_object_or_404', ('subscriptions.change_subscription',)
    for handler in (broadcasts.preview_view, broadcasts.send_test_view, broadcasts.send_view, broadcasts.cancel_view):
        yield handler, ('101',), 'broadcasts.admin.get_object_or_404', ('broadcasts.change_broadcast',)
    yield broadcasts.wizard_new_view, (), 'broadcasts.admin.ensure_wizard_segments', ('broadcasts.add_broadcast',)
    yield broadcasts.wizard_view, ('101', 2), 'broadcasts.admin.ensure_wizard_segments', ('broadcasts.change_broadcast',)
    for handler in (polls.preview_view, polls.send_test_view, polls.send_view, polls.complete_view):
        yield handler, ('101',), 'polls.admin.get_object_or_404', ('polls.change_pollcampaign',)
    yield support.reply_view, ('101',), 'support.admin.get_object_or_404', ('support.change_supportcase',)
    yield billing.force_charge_view, ('101',), 'subscriptions.admin.get_object_or_404', ('subscriptions.change_payment',)
    for handler in (recovery.launch_view, recovery.expand_view, recovery.pause_view):
        yield handler, ('101',), 'subscriptions.admin.get_object_or_404', ('subscriptions.change_trialrecoverycampaign',)


class AdminCustomRouteMatrixTests(SimpleTestCase):
    def test_each_custom_route_denies_unprivileged_before_lookup(self):
        for handler, args, lookup_path, permissions in custom_route_cases():
            for user in (principal(), principal('users.view_telegramuser'), principal(*permissions, staff=False), principal(*permissions, active=False)):
                for method in ('get', 'post'):
                    with self.subTest(handler=handler.__qualname__, method=method, user=user):
                        request = getattr(RequestFactory(), method)('/admin/action/', {'confirm': 'on'})
                        request.user = user
                        with patch(lookup_path, side_effect=LookupReached) as lookup:
                            try:
                                handler(request, *args)
                            except Exception as exc:
                                self.assertIsInstance(exc, PermissionDenied)
                            else:
                                self.fail('Unauthorized handler returned normally')
                        lookup.assert_not_called()

    def test_each_authorized_role_reaches_confirmation_lookup(self):
        for handler, args, lookup_path, permissions in custom_route_cases():
            for user in (principal(*permissions), principal(superuser=True)):
                with self.subTest(handler=handler.__qualname__, superuser=user.is_superuser):
                    request = RequestFactory().get('/admin/action/')
                    request.user = user
                    with patch(lookup_path, side_effect=LookupReached):
                        with self.assertRaises(LookupReached):
                            handler(request, *args)

    def test_manual_message_requires_send_permission_before_user_lookup(self):
        for handler in (admin_pages.manual_message_view, admin_site.manual_message_view):
            request = RequestFactory().post('/admin/manual-message/', {'user_lookup': '101'})
            request.user = principal()
            args = (admin_site, request) if handler is admin_pages.manual_message_view else (request,)
            with patch('common.admin_pages._resolve_user_lookup', side_effect=LookupReached) as lookup:
                try:
                    handler(*args)
                except Exception as exc:
                    self.assertIsInstance(exc, PermissionDenied)
                else:
                    self.fail('Unauthorized manual message page returned normally')
            lookup.assert_not_called()

    def test_payment_bulk_actions_deny_without_change_permission(self):
        for handler in (confirm_manually, reject_payment, mark_refunded):
            for confirmed in ('0', '1'):
                with self.subTest(handler=handler.__name__, confirmed=confirmed):
                    request = RequestFactory().post('/admin/subscriptions/payment/', {'confirm_action': confirmed})
                    request.user = principal('subscriptions.view_payment')
                    queryset = MagicMock()
                    with patch.object(admin_site, 'each_context', return_value={}), patch('subscriptions.admin.messages.success'):
                        with self.assertRaises(PermissionDenied):
                            handler(None, request, queryset)
                    self.assertEqual(queryset.mock_calls, [])

    def test_bulk_tags_deny_before_queryset(self):
        request = RequestFactory().post('/admin/users/telegramuser/', {'bulk_tag_operation': 'assign', 'bulk_tag': '1', '_selected_action': ['101']})
        request.user = principal('users.view_telegramuser')
        with patch('users.admin.Tag.objects.filter', side_effect=LookupReached) as lookup:
            try:
                TelegramUserAdmin(TelegramUser, admin_site)._handle_bulk_tag_post(request)
            except Exception as exc:
                self.assertIsInstance(exc, PermissionDenied)
            else:
                self.fail('Unauthorized bulk tag action returned normally')
        lookup.assert_not_called()


class AdminAuditPrivacyTests(SimpleTestCase):
    def canary_state(self):
        return UserAdminState(telegram_user=TelegramUser(tg_user_id=101), is_test_user=True,
            onboarding_payload={'accounts': [{'balance': '98765.43', 'name': 'PRIVATE-CANARY'}]},
            last_user_input='PRIVATE-CANARY salary 98765.43', last_bot_response='PRIVATE-CANARY balance',
            last_parse_error='PRIVATE-CANARY parse', test_user_notes='QA')

    def test_mark_and_unmark_audits_never_copy_raw_financial_state(self):
        import json
        for operation in (services.mark_user_as_test_user, services.unmark_user_as_test_user):
            state = self.canary_state()
            with self.subTest(operation=operation.__name__):
                with patch('users.services.get_or_create_admin_state', return_value=state), patch.object(state, 'save'), patch('users.services.create_audit_log') as audit:
                    from django.contrib.auth import get_user_model
                    operation(user=TelegramUser(tg_user_id=101), admin_user=get_user_model()(pk=77, is_staff=True, is_active=True, is_superuser=True))
                payload = audit.call_args.kwargs
                serialized = json.dumps({'before': payload['before'], 'after': payload['after']}, default=str)
                self.assertNotIn('PRIVATE-CANARY', serialized)
                self.assertNotIn('98765.43', serialized)
                self.assertEqual(set(payload['before']), {'is_test_user', 'marked_as_test_by_id', 'marked_as_test_at'})
                self.assertEqual(set(payload['after']), set(payload['before']))

    def test_reset_snapshot_is_operational_only_without_financial_queries(self):
        import json
        state = self.canary_state()
        with patch('users.services._latest_subscription', return_value=None), patch('users.services.Transaction.objects.filter') as tx, patch('users.services.Account.objects.filter') as accounts, patch('users.services.Category.objects.filter') as categories, patch('users.services.Debt.objects.filter') as debts:
            snapshot = services._snapshot_user(TelegramUser(tg_user_id=101), state)
        serialized = json.dumps(snapshot, default=str)
        self.assertNotIn('PRIVATE-CANARY', serialized)
        self.assertNotIn('98765.43', serialized)
        self.assertNotIn('counts', snapshot)
        for query in (tx, accounts, categories, debts):
            query.assert_not_called()

    def test_audit_log_requires_explicit_view_permission(self):
        from audit_log.admin import AdminAuditLogAdmin
        from audit_log.models import AdminAuditLog
        model_admin = AdminAuditLogAdmin(AdminAuditLog, admin_site)
        request = RequestFactory().get('/admin/audit_log/adminauditlog/')
        for actor, allowed in ((principal(), False), (principal('audit_log.change_adminauditlog'), False), (principal('audit_log.view_adminauditlog'), True), (principal(superuser=True), True)):
            request.user = actor
            with self.subTest(actor=actor):
                self.assertEqual(model_admin.has_module_permission(request), allowed)
                self.assertEqual(model_admin.has_view_permission(request), allowed)


@override_settings(ENABLE_ADMIN_TEST_TOOLS=True, ADMIN_TEST_TELEGRAM_IDS=[])
class AdminOnboardingDebugPrivacyTests(SimpleTestCase):
    def request(self, method='post', **data):
        request = getattr(RequestFactory(), method)('/admin/onboarding-debug/', data or {'user_lookup': '101', 'reason': 'QA regression'})
        request.user = principal('users.can_use_test_tools')
        return request

    def test_get_and_missing_reason_never_read_debug(self):
        for request in (self.request('get', user_id='101'), self.request(user_lookup='101', reason='   ')):
            with self.subTest(method=request.method):
                with patch('common.admin_pages._resolve_user_lookup', return_value=(TelegramUser(tg_user_id=101), [], None)), patch('common.admin_pages._build_debug_context', return_value={'private': 'CANARY'}) as build, patch.object(admin_site, 'each_context', return_value={}):
                    response = admin_pages.onboarding_debug_view(admin_site, request)
                self.assertIsNone(response.context_data['debug'])
                build.assert_not_called()

    def test_non_test_target_denied_before_raw_queries(self):
        target = TelegramUser(tg_user_id=101)
        with patch('common.admin_pages._resolve_user_lookup', return_value=(target, [], None)), patch('common.admin_pages.UserAdminState.objects.filter') as states, patch('common.admin_pages.BotEvent.objects.filter') as events, patch.object(admin_site, 'each_context', return_value={}):
            states.return_value.values_list.return_value.first.return_value = False
            with self.assertRaises(PermissionDenied):
                admin_pages.onboarding_debug_view(admin_site, self.request())
            states.return_value.first.assert_not_called()
            events.assert_not_called()

    def test_authorized_test_target_is_audited_before_raw_read(self):
        import json
        target = TelegramUser(tg_user_id=101)
        state = UserAdminState(telegram_user=target, is_test_user=True, last_user_input='DEBUG-PRIVATE-CANARY')
        order = []
        with patch('common.admin_pages._resolve_user_lookup', return_value=(target, [], None)), patch('common.admin_pages.UserAdminState.objects.filter') as states, patch('common.admin_pages.BotEvent.objects.filter') as events, patch('common.admin_pages.create_audit_log', side_effect=lambda **kw: order.append('audit')) as audit, patch.object(admin_site, 'each_context', return_value={}):
            states.return_value.values_list.return_value.first.return_value = True
            states.return_value.first.side_effect = lambda: order.append('raw_read') or state
            events.return_value.order_by.return_value.__getitem__.return_value = []
            response = admin_pages.onboarding_debug_view(admin_site, self.request())
        self.assertEqual(order, ['audit', 'raw_read'])
        self.assertEqual(response.context_data['debug']['state'].last_user_input, 'DEBUG-PRIVATE-CANARY')
        audit.assert_called_once()
        data = audit.call_args.kwargs
        self.assertEqual(data['reason'], 'QA regression')
        self.assertEqual(data['target_user_id'], 101)
        self.assertNotIn('DEBUG-PRIVATE-CANARY', json.dumps(data, default=str))

    def test_audit_failure_blocks_raw_read(self):
        target = TelegramUser(tg_user_id=101)
        with patch('common.admin_pages._resolve_user_lookup', return_value=(target, [], None)), patch('common.admin_pages.UserAdminState.objects.filter') as states, patch('common.admin_pages.BotEvent.objects.filter') as events, patch('common.admin_pages.create_audit_log', side_effect=RuntimeError('audit unavailable')), patch.object(admin_site, 'each_context', return_value={}):
            states.return_value.values_list.return_value.first.return_value = True
            with self.assertRaisesRegex(RuntimeError, 'audit unavailable'):
                admin_pages.onboarding_debug_view(admin_site, self.request())
            states.return_value.first.assert_not_called()
            events.assert_not_called()


class SyntheticPrincipalMiddleware:
    """Test-only auth adapter; real admin URLs/CSRF/RBAC remain in use."""
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        from django.contrib.auth.models import AnonymousUser
        request.user = request.META.get('ADMIN_TEST_PRINCIPAL', AnonymousUser())
        request.session = {}
        return self.get_response(request)


@override_settings(MIDDLEWARE=['common.tests_admin_security_batch1.SyntheticPrincipalMiddleware'],
                   DEBUG=False, MINIAPP_OPERATOR_TELEGRAM_IDS=[])
class AdminHttpRoleMatrixTests(SimpleTestCase):
    def setUp(self):
        import logging
        # Denied routes and deliberate sentinels are expected, not server faults.
        for name in ('django.request', 'django.security.csrf'):
            mute = patch.object(logging.getLogger(name), 'disabled', True)
            mute.start()
            self.addCleanup(mute.stop)

    def route_cases(self):
        from django.urls import reverse
        for handler, args, boundary, permissions in custom_route_cases():
            routes = [route for route in handler.__self__.get_urls()
                      if getattr(route.callback, '__name__', '') == handler.__name__]
            self.assertEqual(len(routes), 1, handler.__qualname__)
            yield reverse('admin:' + routes[0].name, args=args), boundary, permissions

    def test_real_urls_preserve_login_boundary_and_enforce_role_matrix(self):
        from django.contrib.auth.models import AnonymousUser
        from django.test import Client
        client = Client()
        actors = [
            AnonymousUser(), principal(staff=False), principal(active=False), principal(),
            principal('users.view_telegramuser'), principal('support.change_supportcase'),
            principal('subscriptions.change_payment', 'subscriptions.change_subscription'),
            principal(superuser=True),
        ]
        for url, boundary, permissions in self.route_cases():
            for actor in actors:
                for method in ('get', 'post'):
                    with self.subTest(url=url, actor=actor, method=method):
                        allowed = actor.is_active and actor.is_staff and (
                            actor.is_superuser or all(actor.has_perm(p) for p in permissions))
                        with patch(boundary, side_effect=LookupReached) as lookup:
                            if allowed:
                                with self.assertRaises(LookupReached):
                                    getattr(client, method)(url, {'confirm': 'on'}, ADMIN_TEST_PRINCIPAL=actor)
                            else:
                                response = getattr(client, method)(url, {'confirm': 'on'}, ADMIN_TEST_PRINCIPAL=actor)
                                self.assertEqual(response.status_code, 403 if actor.is_active and actor.is_staff else 302)
                                if response.status_code == 302:
                                    self.assertIn('login/?next=', response.url)
                                lookup.assert_not_called()

    def test_csrf_still_blocks_privileged_post_before_handler(self):
        from django.test import Client
        client = Client(enforce_csrf_checks=True)
        for url, boundary, _ in self.route_cases():
            with self.subTest(url=url):
                with patch(boundary, side_effect=LookupReached) as lookup:
                    response = client.post(url, {'confirm': 'on'}, ADMIN_TEST_PRINCIPAL=principal(superuser=True))
                self.assertEqual(response.status_code, 403)
                lookup.assert_not_called()


class AdminNeighborBoundaryTests(SimpleTestCase):
    def test_cleanup_mode_requires_cleanup_role_before_target_lookup(self):
        with override_settings(ENABLE_ADMIN_TEST_TOOLS=True):
            for handler, args, boundary, field in (
                (TelegramUserAdmin(TelegramUser, admin_site).reset_onboarding_view, ('101',), 'users.admin.get_object_or_404', 'mode'),
                (admin_site.reset_my_onboarding_view, (), 'common.admin_pages._my_test_users_queryset', 'mode'),
                (admin_site.qa_tools_view, (), 'common.admin_pages._my_test_users_queryset', 'reset-mode'),
            ):
                request = RequestFactory().post('/admin/action/', {field: 'cleanup', 'action': 'reset_my_onboarding'})
                request.user = principal('users.can_use_test_tools')
                with self.subTest(handler=handler.__qualname__):
                    with patch(boundary, side_effect=LookupReached) as lookup:
                        with self.assertRaises(PermissionDenied):
                            handler(request, *args)
                    lookup.assert_not_called()

    def test_tag_services_enforce_their_own_permissions(self):
        from users.models import Tag
        for operation, kwargs, lookup in (
            (services.get_or_create_user_tag, {'name': 'QA'}, 'users.services.Tag.objects.filter'),
            (services.assign_tag_to_users, {'tag': Tag(pk=1), 'users': [TelegramUser(tg_user_id=101)]}, 'users.services.UserTag.objects.filter'),
            (services.remove_tag_from_users, {'tag': Tag(pk=1), 'users': [TelegramUser(tg_user_id=101)]}, 'users.services.UserTag.objects.filter'),
        ):
            with self.subTest(operation=operation.__name__):
                with patch(lookup, side_effect=LookupReached) as query:
                    with self.assertRaises(PermissionDenied):
                        operation(admin_user=principal(), **kwargs)
                query.assert_not_called()

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[999])
    def test_operator_reset_and_delete_denied_before_lookup(self):
        for operation in (
            lambda: services.reset_user_onboarding(999, 'safe', principal(superuser=True), 'QA'),
            lambda: services.hard_delete_user(999, admin_user=principal(superuser=True), reason='QA'),
        ):
            with patch('users.services.TelegramUser.objects.filter', side_effect=LookupReached) as query:
                with self.assertRaises(PermissionDenied):
                    operation()
                query.assert_not_called()

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[999])
    def test_operator_ban_denied_before_state_read(self):
        request = RequestFactory().post('/admin/action/', {'confirm': 'on'})
        request.user = principal(superuser=True)
        with patch('users.admin.get_object_or_404', return_value=TelegramUser(tg_user_id=999)), patch('users.admin.UserAdminState.objects.get_or_create', side_effect=LookupReached) as state:
            with self.assertRaises(PermissionDenied):
                TelegramUserAdmin(TelegramUser, admin_site).set_status_view(request, '999', 'banned')
            state.assert_not_called()

    def test_health_and_notifications_require_explicit_roles(self):
        for handler in (admin_pages.healthcheck_view, admin_pages.admin_notifications_view):
            for method in ('get', 'post'):
                request = getattr(RequestFactory(), method)('/admin/action/')
                request.user = principal()
                with self.subTest(handler=handler.__name__, method=method):
                    with patch('common.admin_pages.run_admin_healthcheck', side_effect=LookupReached) as health, patch('common.admin_pages.send_admin_notification_task.delay', side_effect=LookupReached) as send, patch('common.admin_pages._check_telegram_api', side_effect=LookupReached) as api, patch.object(admin_site, 'each_context', return_value={}):
                        with self.assertRaises(PermissionDenied):
                            handler(admin_site, request)
                        for boundary in (health, send, api):
                            boundary.assert_not_called()

    def test_custom_navigation_uses_action_permissions(self):
        for key in ('custom:manual_message', 'custom:healthcheck', 'custom:admin_notifications', 'custom:bot_errors'):
            request = RequestFactory().get('/admin/')
            request.user = principal()
            with self.subTest(key=key):
                self.assertIsNone(admin_site._build_custom_item(request, key, 'Protected'))

    def test_crm_and_recovery_read_permission_precedes_lookup(self):
        request = RequestFactory().get('/admin/action/')
        request.user = principal()
        for handler, boundary in (
            (TelegramUserAdmin(TelegramUser, admin_site).change_view, 'users.admin.TelegramUserAdmin.get_queryset'),
            (TrialRecoveryCampaignAdmin(TrialRecoveryCampaign, admin_site).change_view, 'subscriptions.admin.get_object_or_404'),
        ):
            with self.subTest(handler=handler.__qualname__):
                with patch(boundary, side_effect=LookupReached) as query:
                    with self.assertRaises(PermissionDenied):
                        handler(request, '101')
                query.assert_not_called()


class AdminServicePermissionTests(SimpleTestCase):
    def test_test_mark_denied_at_service_before_state_lookup(self):
        for operation in (services.mark_user_as_test_user, services.unmark_user_as_test_user):
            with self.subTest(operation=operation.__name__):
                with patch('users.services.get_or_create_admin_state', side_effect=LookupReached) as lookup:
                    try:
                        operation(user=TelegramUser(tg_user_id=101), admin_user=principal())
                    except Exception as exc:
                        self.assertIsInstance(exc, PermissionDenied)
                    else:
                        self.fail('Unauthorized service returned normally')
                lookup.assert_not_called()

    def test_mark_permission_does_not_grant_cleanup(self):
        with patch('users.services._latest_subscription', return_value=SimpleNamespace(status='paid')):
            for actor in (principal(), principal('users.can_use_test_tools', 'users.change_useradminstate')):
                with self.subTest(actor=actor):
                    with self.assertRaises(PermissionDenied):
                        services._validate_reset_mode(user=TelegramUser(tg_user_id=101), state=UserAdminState(is_test_user=True),
                            admin_user=actor, mode='cleanup', options={'reason': 'QA', 'double_confirmed': True})

    def test_reset_denies_before_read_or_create_even_with_forged_operator_options(self):
        for actor in (principal(), None):
            with self.subTest(actor=actor):
                with patch('users.services.TelegramUser.objects.filter', side_effect=LookupReached) as lookup:
                    try:
                        services.reset_user_onboarding(101, 'cleanup', actor, 'QA', options={
                            'operator_authorized': True, 'operator_tg_user_id': 999,
                            'double_confirmed': True, 'dry_run': True})
                    except Exception as exc:
                        self.assertIsInstance(exc, PermissionDenied)
                    else:
                        self.fail('Caller-supplied operator flag authorized reset')
                lookup.assert_not_called()

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[999])
    def test_authenticated_miniapp_operator_keeps_service_access(self):
        request = RequestFactory().post('/app/api/operator/users/101/action/')
        request.session = {'miniapp_tg_user_id': 999}
        with patch('users.services.get_or_create_admin_state', side_effect=LookupReached):
            with self.assertRaises(LookupReached):
                services.mark_user_as_test_user(user=TelegramUser(tg_user_id=101), admin_user=None, request=request)
        with patch('users.services.TelegramUser.objects.filter', side_effect=LookupReached):
            with self.assertRaises(LookupReached):
                services.reset_user_onboarding(101, 'safe', None, 'QA', options={'request': request})
