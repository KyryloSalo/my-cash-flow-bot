from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.http import HttpResponse
from django.template.loader import get_template
from django.test import RequestFactory, SimpleTestCase, override_settings

from accounts.models import Account, AccountAdminState
from bot_events.models import BotEvent
from categories.models import Category
from common.admin import ADMIN_PAGE_METADATA
from common.admin_site import admin_site
from common.middleware import AdminIPAllowlistMiddleware, _trusted_remote_ip
from common.telegram_admins import get_primary_admin_telegram_id
from transactions.models import Debt, DebtPayment, Transaction


class AllowAllAdminUser:
    is_authenticated = True
    is_active = True
    is_staff = True
    is_superuser = True
    pk = 1

    def has_perm(self, _permission):
        return True

    def has_module_perms(self, _app_label):
        return True


class AdminIPAllowlistMiddlewareTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.middleware = AdminIPAllowlistMiddleware(lambda request: HttpResponse("ok"))

    @override_settings(ADMIN_TRUSTED_PROXY_IPS=["172.18.0.10"])
    def test_trusted_proxy_prefers_x_real_ip(self):
        request = self.factory.get("/", REMOTE_ADDR="172.18.0.10", HTTP_X_REAL_IP="203.0.113.10", HTTP_X_FORWARDED_FOR="198.51.100.1, 198.51.100.2")

        self.assertEqual(_trusted_remote_ip(request), "203.0.113.10")

    @override_settings(ADMIN_TRUSTED_PROXY_IPS=["172.18.0.10"])
    def test_trusted_proxy_ignores_forwarded_for_without_real_ip(self):
        request = self.factory.get("/", REMOTE_ADDR="172.18.0.10", HTTP_X_FORWARDED_FOR="198.51.100.1, 203.0.113.10")

        self.assertEqual(_trusted_remote_ip(request), "172.18.0.10")

    @override_settings(ADMIN_TRUSTED_PROXY_IPS=[])
    def test_untrusted_peer_cannot_assert_x_real_ip(self):
        request = self.factory.get("/", REMOTE_ADDR="198.51.100.77", HTTP_X_REAL_IP="203.0.113.10")

        self.assertEqual(_trusted_remote_ip(request), "198.51.100.77")

    @override_settings(ADMIN_IP_ALLOWLIST=["203.0.113.10"], STATIC_URL="/static/")
    def test_spoofed_first_forwarded_ip_does_not_bypass_allowlist(self):
        request = self.factory.get("/", REMOTE_ADDR="172.18.0.10", HTTP_X_FORWARDED_FOR="203.0.113.10, 198.51.100.77")

        response = self.middleware(request)

        self.assertEqual(response.status_code, 403)

    @override_settings(
        ADMIN_IP_ALLOWLIST=["203.0.113.10"],
        ADMIN_TRUSTED_PROXY_IPS=["172.18.0.10"],
        STATIC_URL="/static/",
    )
    def test_real_ip_header_allows_request_when_trusted_proxy_sets_client_ip(self):
        request = self.factory.get("/", REMOTE_ADDR="172.18.0.10", HTTP_X_REAL_IP="203.0.113.10", HTTP_X_FORWARDED_FOR="198.51.100.77")

        response = self.middleware(request)

        self.assertEqual(response.status_code, 200)

    @override_settings(ADMIN_IP_ALLOWLIST=["203.0.113.10"], STATIC_URL="/static/")
    def test_billing_paths_still_bypass_allowlist(self):
        request = self.factory.get("/billing/mono/webhook", REMOTE_ADDR="172.18.0.10", HTTP_X_FORWARDED_FOR="198.51.100.77")

        response = self.middleware(request)

        self.assertEqual(response.status_code, 200)


class PrimaryAdminTelegramIdTests(SimpleTestCase):
    @override_settings(ADMIN_TELEGRAM_IDS=[111, 222], PRIMARY_ADMIN_TELEGRAM_ID=999)
    def test_primary_admin_override_wins(self):
        self.assertEqual(get_primary_admin_telegram_id(), 999)

    @override_settings(ADMIN_TELEGRAM_IDS=[111, 222], PRIMARY_ADMIN_TELEGRAM_ID=None)
    def test_primary_admin_falls_back_to_first_admin(self):
        self.assertEqual(get_primary_admin_telegram_id(), 111)

    @override_settings(ADMIN_TELEGRAM_IDS=[], PRIMARY_ADMIN_TELEGRAM_ID=None)
    def test_primary_admin_returns_none_when_admins_missing(self):
        self.assertIsNone(get_primary_admin_telegram_id())


class OperationalAdminArchitectureTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def test_main_navigation_uses_operator_task_order(self):
        section_names = [section.label for section in admin_site.menu_structure]

        self.assertEqual(
            section_names,
            [
                "Робочий стіл",
                "Користувачі",
                "Монетизація",
                "Комунікації",
                "Система",
            ],
        )

    def test_primary_menu_labels_are_explicit_and_complete(self):
        actual = {
            section.label: [label for _model_key, label in section.items]
            for section in admin_site.menu_structure
        }

        self.assertEqual(
            actual,
            {
                "Робочий стіл": ["Щоденний контроль"],
                "Користувачі": ["Усі користувачі", "Теги користувачів", "Партнерські посилання"],
                "Монетизація": [
                    "Підписки користувачів",
                    "Оплати Vydno",
                    "Картки та автосписання",
                    "Тарифи",
                    "Промокоди та акції",
                    "Recovery trial-користувачів",
                ],
                "Комунікації": [
                    "Написати користувачу",
                    "Розсилки",
                    "Опитування",
                    "Звернення користувачів",
                    "Відгуки користувачів",
                    "Теми push-сповіщень",
                    "Історія повідомлень",
                ],
                "Система": [
                    "Стан системи",
                    "Помилки бота",
                    "Налаштування бота",
                    "Сповіщення адміністраторів",
                    "Події бота",
                    "Журнал дій",
                    "Адміністратори",
                ],
            },
        )

    def test_private_financial_registries_are_absent_from_navigation(self):
        model_keys = {
            model_key
            for section in (*admin_site.menu_structure, *admin_site.technical_menu_structure)
            for model_key, _label in section.items
        }

        self.assertTrue(
            {
                "accounts.Account",
                "accounts.AccountAdminState",
                "transactions.Transaction",
                "transactions.Debt",
                "transactions.DebtPayment",
                "categories.Category",
            }.isdisjoint(model_keys)
        )

    def test_every_main_registry_has_operator_description(self):
        model_keys = {
            model_key
            for section in admin_site.menu_structure
            for model_key, _label in section.items
            if "." in model_key and not model_key.startswith("custom:")
        }

        missing = sorted(model_key for model_key in model_keys if model_key not in ADMIN_PAGE_METADATA)

        self.assertEqual(missing, [])
        self.assertTrue(all(ADMIN_PAGE_METADATA[key].get("description") for key in model_keys))

    def test_menu_labels_match_page_titles_and_sections(self):
        for section in (*admin_site.menu_structure, *admin_site.technical_menu_structure):
            for model_key, label in section.items:
                if model_key not in ADMIN_PAGE_METADATA:
                    continue
                with self.subTest(model=model_key):
                    self.assertEqual(ADMIN_PAGE_METADATA[model_key]["title"], label)
                    self.assertEqual(ADMIN_PAGE_METADATA[model_key]["section"], section.label)

    def test_every_registered_model_has_operational_contract(self):
        for model, model_admin in admin_site._registry.items():
            with self.subTest(model=model._meta.label):
                metadata = ADMIN_PAGE_METADATA.get(model._meta.label)
                self.assertIsNotNone(metadata)
                self.assertTrue(metadata.get("section"))
                self.assertTrue(metadata.get("description"))
                self.assertGreaterEqual(len(model_admin.get_list_display(None)), 3)
                self.assertTrue(getattr(model_admin, "search_fields", ()))
                self.assertLessEqual(model_admin.list_per_page, 25)

    @override_settings(ENABLE_ADMIN_DEVELOPER_MODE=False)
    def test_operator_menu_contains_only_expected_sections(self):
        request = self.factory.get("/admin/")
        request.user = AllowAllAdminUser()

        sections = admin_site.get_app_list(request)

        self.assertEqual(
            [section["name"] for section in sections],
            [section.label for section in admin_site.menu_structure],
        )
        self.assertEqual([section["app_label"] for section in sections], [section.key for section in admin_site.menu_structure])
        self.assertNotIn("Технічне", [section["name"] for section in sections])
        visible_labels = {
            model["name"]
            for section in sections
            for model in section["models"]
        }
        self.assertIn("Відгуки користувачів", visible_labels)
        self.assertIn("Адміністратори", visible_labels)
        self.assertNotIn("Рахунки користувачів", visible_labels)
        self.assertNotIn("Доходи, витрати й перекази", visible_labels)

        for section, rendered_section in zip(admin_site.menu_structure, sections, strict=True):
            self.assertEqual(rendered_section["app_url"], rendered_section["models"][0]["admin_url"])

    @override_settings(ENABLE_ADMIN_DEVELOPER_MODE=True)
    def test_developer_mode_appends_technical_section(self):
        request = self.factory.get("/admin/")
        request.user = AllowAllAdminUser()

        sections = admin_site.get_app_list(request)

        self.assertEqual(sections[-1]["name"], "Технічне")

    def test_private_financial_registries_deny_ordinary_admin_access(self):
        request = self.factory.get("/admin/")
        request.user = AllowAllAdminUser()

        for model in (Account, AccountAdminState, Transaction, Debt, DebtPayment, Category):
            model_admin = admin_site._registry[model]
            with self.subTest(model=model._meta.label):
                self.assertFalse(model_admin.has_module_permission(request))
                self.assertFalse(model_admin.has_view_permission(request))
                self.assertFalse(model_admin.has_add_permission(request))
                self.assertFalse(model_admin.has_change_permission(request))
                self.assertFalse(model_admin.has_delete_permission(request))
                self.assertFalse(any(model_admin.get_model_perms(request).values()))

    def test_user_card_and_event_admin_do_not_expose_raw_finance_payloads(self):
        template_source = (Path(settings.BASE_DIR) / "templates" / "admin" / "user_crm_detail.html").read_text(
            encoding="utf-8"
        )
        for forbidden_marker in (
            "crm.transactions",
            "crm.accounts",
            "crm.categories",
            "event.raw_input",
            "onboarding_payload",
            "Всі транзакції користувача",
        ):
            with self.subTest(marker=forbidden_marker):
                self.assertNotIn(forbidden_marker, template_source)

        bot_event_admin = admin_site._registry[BotEvent]
        self.assertNotIn("raw_input", bot_event_admin.search_fields)
        self.assertIn("raw_input", bot_event_admin.exclude)
        self.assertIn("parsed_result", bot_event_admin.exclude)
        request = self.factory.get("/admin/bot_events/botevent/")
        request.user = AllowAllAdminUser()
        self.assertFalse(bot_event_admin.has_add_permission(request))
        self.assertFalse(bot_event_admin.has_change_permission(request))
        self.assertFalse(bot_event_admin.has_delete_permission(request))

    def test_operational_templates_compile(self):
        for template_name in (
            "admin/base_site.html",
            "admin/bulk_action_confirmation.html",
            "admin/object_action_form.html",
            "admin/payment_action_confirmation.html",
            "dashboard/index.html",
        ):
            with self.subTest(template_name=template_name):
                self.assertIsNotNone(get_template(template_name))

    def test_every_project_admin_template_compiles(self):
        templates_root = Path(settings.BASE_DIR) / "templates"
        template_names = sorted(
            path.relative_to(templates_root).as_posix()
            for path in templates_root.rglob("*.html")
            if path.relative_to(templates_root).parts[0] in {"admin", "dashboard"}
        )

        self.assertTrue(template_names)
        for template_name in template_names:
            with self.subTest(template_name=template_name):
                self.assertIsNotNone(get_template(template_name))
