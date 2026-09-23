from __future__ import annotations

from contextlib import nullcontext
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import RequestFactory, SimpleTestCase, override_settings
from django.urls import resolve

from common.admin import ADMIN_PAGE_METADATA
from common.admin_navigation import PRIMARY_MENU_SECTIONS
from common.admin_site import admin_site
from miniapp import funnel, funnel_views, models


class PartnerLinkSchemaTests(SimpleTestCase):
    def test_partner_link_model_keeps_campaign_dimensions_and_status(self) -> None:
        self.assertTrue(hasattr(models, "PartnerLink"))
        fields = {field.name for field in models.PartnerLink._meta.fields}
        self.assertTrue(
            {
                "name",
                "code",
                "destination",
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_content",
                "utm_term",
                "notes",
                "is_active",
                "created_at",
                "updated_at",
            }.issubset(fields)
        )

    def test_acquisition_session_keeps_first_and_last_touch_links(self) -> None:
        session_fields = {field.name for field in models.AcquisitionSession._meta.fields}

        self.assertTrue(
            {
                "first_partner_link",
                "last_partner_link",
                "last_partner_link_clicked_at",
            }.issubset(session_fields)
        )

    def test_partner_click_has_link_session_and_timestamp(self) -> None:
        self.assertTrue(hasattr(models, "PartnerLinkClick"))
        click_fields = {field.name for field in models.PartnerLinkClick._meta.fields}

        self.assertTrue(
            {"partner_link", "acquisition_session", "clicked_at"}.issubset(click_fields)
        )

    def test_funnel_event_freezes_first_and_last_touch_links_at_conversion_time(self) -> None:
        event_fields = {field.name for field in models.FunnelEvent._meta.fields}

        self.assertTrue(
            {"first_partner_link", "last_partner_link"}.issubset(event_fields)
        )

    def test_migrations_define_event_touch_columns_once(self) -> None:
        migration_modules = (
            import_module("miniapp.migrations.0009_partnerlink_partnerlinkclick_and_more"),
            import_module("miniapp.migrations.0010_funnelevent_first_partner_link_and_more"),
        )

        event_touch_fields = [
            (operation.name, operation.field.__class__.__name__)
            for module in migration_modules
            for operation in module.Migration.operations
            if operation.__class__.__name__ == "AddField"
            and operation.model_name == "funnelevent"
            and operation.name in {"first_partner_link", "last_partner_link"}
        ]

        self.assertCountEqual(
            event_touch_fields,
            [
                ("first_partner_link", "ForeignKey"),
                ("last_partner_link", "ForeignKey"),
            ],
        )


class PartnerLinkUrlTests(SimpleTestCase):
    @override_settings(DOMAIN="example.test")
    def test_public_url_uses_generated_code_on_public_domain(self) -> None:
        link = models.PartnerLink(name="Telegram creator", code="tg_creator", utm_source="telegram")

        self.assertEqual(link.public_url, "https://example.test/app/r/tg_creator")

    def test_destination_path_is_restricted_to_known_local_routes(self) -> None:
        landing = models.PartnerLink(destination=models.PartnerLink.Destination.LANDING)
        app = models.PartnerLink(destination=models.PartnerLink.Destination.APP)

        self.assertEqual(landing.destination_path, "/")
        self.assertEqual(app.destination_path, "/app/")


class PartnerLinkTrackingTests(SimpleTestCase):
    def test_first_click_sets_first_and_last_touch_and_campaign_dimensions(self) -> None:
        self.assertTrue(hasattr(funnel, "track_partner_link_click"))
        acquisition = SimpleNamespace(
            pk="0b387247-f643-4e4c-8e6f-55998c22be20",
            first_partner_link_id=None,
            last_partner_link_id=None,
            last_partner_link_clicked_at=None,
            utm_source="",
            utm_medium="",
            utm_campaign="",
            utm_content="",
            utm_term="",
            referral_code="",
            save=MagicMock(),
        )
        link = models.PartnerLink(
            id=7,
            name="YouTube creator",
            code="yt_creator",
            utm_source="youtube",
            utm_medium="creator",
            utm_campaign="launch",
            utm_content="video_1",
            utm_term="budgeting",
        )
        click = object()

        with (
            patch("miniapp.funnel.get_or_create_acquisition_session", return_value=acquisition),
            patch("miniapp.funnel._lock_acquisition_session", return_value=acquisition),
            patch("miniapp.funnel.PartnerLinkClick.objects.create", return_value=click) as create_click,
        ):
            result = funnel._track_partner_link_click_locked(SimpleNamespace(), link=link)

        self.assertIs(result[0], acquisition)
        self.assertIs(result[1], click)
        self.assertEqual(acquisition.first_partner_link_id, link.pk)
        self.assertEqual(acquisition.last_partner_link_id, link.pk)
        self.assertIsNotNone(acquisition.last_partner_link_clicked_at)
        self.assertEqual(acquisition.utm_source, "youtube")
        self.assertEqual(acquisition.utm_medium, "creator")
        self.assertEqual(acquisition.utm_campaign, "launch")
        self.assertEqual(acquisition.utm_content, "video_1")
        self.assertEqual(acquisition.utm_term, "budgeting")
        self.assertEqual(acquisition.referral_code, "yt_creator")
        create_click.assert_called_once_with(partner_link=link, acquisition_session=acquisition)

    def test_later_link_changes_only_last_touch_and_keeps_first_campaign(self) -> None:
        acquisition = SimpleNamespace(
            pk="0b387247-f643-4e4c-8e6f-55998c22be20",
            first_partner_link_id=3,
            last_partner_link_id=3,
            last_partner_link_clicked_at=None,
            utm_source="instagram",
            utm_medium="creator",
            utm_campaign="launch_a",
            utm_content="reel_1",
            utm_term="budgeting",
            referral_code="ig_creator",
            save=MagicMock(),
        )
        later_link = models.PartnerLink(
            id=7,
            name="YouTube creator",
            code="yt_creator",
            utm_source="youtube",
            utm_medium="creator",
            utm_campaign="launch_b",
        )

        with (
            patch("miniapp.funnel.get_or_create_acquisition_session", return_value=acquisition),
            patch("miniapp.funnel._lock_acquisition_session", return_value=acquisition),
            patch("miniapp.funnel.PartnerLinkClick.objects.create"),
        ):
            funnel._track_partner_link_click_locked(SimpleNamespace(), link=later_link)

        self.assertEqual(acquisition.first_partner_link_id, 3)
        self.assertEqual(acquisition.last_partner_link_id, 7)
        self.assertEqual(acquisition.utm_source, "instagram")
        self.assertEqual(acquisition.utm_campaign, "launch_a")

    def test_invalid_admin_attribution_is_not_copied_to_session(self) -> None:
        acquisition = SimpleNamespace(
            pk="0b387247-f643-4e4c-8e6f-55998c22be20",
            first_partner_link_id=None,
            last_partner_link_id=None,
            last_partner_link_clicked_at=None,
            utm_source="",
            utm_medium="",
            utm_campaign="",
            utm_content="",
            utm_term="",
            referral_code="",
            save=MagicMock(),
        )
        link = models.PartnerLink(
            id=7,
            name="Unsafe import",
            code="safe_code",
            utm_source="https://tracker.example/private",
            utm_medium="creator@example.test",
        )

        with (
            patch("miniapp.funnel.get_or_create_acquisition_session", return_value=acquisition),
            patch("miniapp.funnel._lock_acquisition_session", return_value=acquisition),
            patch("miniapp.funnel.PartnerLinkClick.objects.create"),
        ):
            funnel._track_partner_link_click_locked(SimpleNamespace(), link=link)

        self.assertEqual(acquisition.utm_source, "")
        self.assertEqual(acquisition.utm_medium, "")
        self.assertEqual(acquisition.referral_code, "safe_code")

    def test_server_event_snapshots_touch_links_from_acquisition(self) -> None:
        payload = {
            "event_name": "registration_completed",
            "funnel_version": "pwa_v1",
            "landing_variant": "",
            "offer_variant": "",
            "platform": "ios",
            "container": "browser",
            "outcome": "",
            "placement": "",
        }
        acquisition = SimpleNamespace(
            first_partner_link_id=3,
            last_partner_link_id=7,
        )
        event = SimpleNamespace(payload_hash=funnel._event_payload_hash(payload))

        with (
            patch("miniapp.funnel.transaction.atomic", return_value=nullcontext()),
            patch(
                "miniapp.funnel.FunnelEvent.objects.get_or_create",
                return_value=(event, True),
            ) as get_or_create,
        ):
            funnel._create_event_once(
                acquisition=acquisition,
                idempotency_key="registration:1001",
                source=models.FunnelEvent.Source.SERVER,
                payload=payload,
            )

        defaults = get_or_create.call_args.kwargs["defaults"]
        self.assertEqual(defaults["first_partner_link_id"], 3)
        self.assertEqual(defaults["last_partner_link_id"], 7)


class PartnerLinkRedirectTests(SimpleTestCase):
    def test_public_partner_route_resolves_to_tracking_redirect(self) -> None:
        match = resolve("/app/r/tg_creator")

        self.assertIs(match.func, funnel_views.partner_link_redirect)

    def test_active_link_click_is_tracked_before_local_redirect(self) -> None:
        self.assertTrue(hasattr(funnel_views, "partner_link_redirect"))
        request = RequestFactory().get("/app/r/tg_creator")
        link = models.PartnerLink(
            id=7,
            name="Telegram creator",
            code="tg_creator",
            utm_source="telegram",
            destination=models.PartnerLink.Destination.LANDING,
        )

        with (
            patch("miniapp.funnel_views.get_object_or_404", return_value=link) as get_link,
            patch("miniapp.funnel_views.track_partner_link_click") as track_click,
        ):
            response = funnel_views.partner_link_redirect(request, code="tg_creator")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/")
        get_link.assert_called_once_with(models.PartnerLink, code="tg_creator", is_active=True)
        track_click.assert_called_once_with(request, link=link)
        self.assertIn("no-cache", response["Cache-Control"])

    def test_tracking_failure_does_not_block_the_redirect(self) -> None:
        request = RequestFactory().get("/app/r/tg_creator")
        link = models.PartnerLink(
            id=7,
            name="Telegram creator",
            code="tg_creator",
            utm_source="telegram",
            destination=models.PartnerLink.Destination.APP,
        )

        with (
            patch("miniapp.funnel_views.get_object_or_404", return_value=link),
            patch(
                "miniapp.funnel_views.track_partner_link_click",
                side_effect=RuntimeError("telemetry unavailable"),
            ),
            patch("miniapp.funnel_views.logger.exception") as log_exception,
        ):
            response = funnel_views.partner_link_redirect(request, code="tg_creator")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/app/")
        log_exception.assert_called_once()


class PartnerLinkRegistrationAttributionTests(SimpleTestCase):
    def test_new_oidc_registration_records_trusted_registration_event(self) -> None:
        request = RequestFactory().get("/app/auth/telegram/callback?state=state&code=code")
        request.session = {}
        pending = SimpleNamespace(
            code="code",
            code_verifier="verifier",
            nonce="nonce",
            return_to="/app/",
        )
        user = SimpleNamespace(tg_user_id=1001)
        registration_identity = object()
        registration = SimpleNamespace(user=user, identity=registration_identity, created=True)
        oidc_identity = SimpleNamespace(claims={"iat": 1_700_000_100})

        with (
            patch("miniapp.views.consume_pending_authorization", return_value=pending),
            patch("miniapp.views.exchange_authorization_code", return_value="id-token"),
            patch("miniapp.views.verify_id_token", return_value=oidc_identity),
            patch("miniapp.views.consume_id_token_once"),
            patch("miniapp.views.bootstrap_telegram_identity", return_value=registration),
            patch("miniapp.views.login_session"),
            patch("miniapp.views.queue_registration_event") as record_event,
        ):
            response = __import__("miniapp.views", fromlist=["views"]).telegram_oidc_callback(request)

        self.assertEqual(response.status_code, 302)
        record_event.assert_called_once_with(request, user=user)
        self.assertIn("registration_completed", funnel.SERVER_EVENT_NAMES)

    def test_new_miniapp_registration_records_trusted_registration_event(self) -> None:
        request = RequestFactory().post(
            "/app/api/auth/telegram",
            data='{"init_data":"signed"}',
            content_type="application/json",
        )
        request.session = {}
        identity = SimpleNamespace(
            tg_user_id=1002,
            first_name="Olena",
            last_name="",
            username="olena",
            raw_user={"language_code": "uk"},
            auth_date=1_700_000_200,
        )
        user = SimpleNamespace(
            tg_user_id=1002,
            first_name="Olena",
            base_currency="UAH",
            lang="uk",
        )
        registration = SimpleNamespace(user=user, identity=object(), created=True)

        with (
            patch("miniapp.views.validate_telegram_init_data", return_value=identity),
            patch("miniapp.views.bootstrap_telegram_identity", return_value=registration),
            patch("miniapp.views.login_session"),
            patch("miniapp.views.resolve_access", return_value={"mode": "active"}),
            patch("miniapp.views.queue_registration_event") as record_event,
        ):
            response = __import__("miniapp.views", fromlist=["views"]).auth_telegram(request)

        self.assertEqual(response.status_code, 200)
        record_event.assert_called_once_with(request, user=user)

    def test_existing_miniapp_user_does_not_count_as_a_registration(self) -> None:
        request = RequestFactory().post(
            "/app/api/auth/telegram",
            data='{"init_data":"signed"}',
            content_type="application/json",
        )
        request.session = {}
        identity = SimpleNamespace(
            tg_user_id=1003,
            first_name="Taras",
            last_name="",
            username="taras",
            raw_user={"language_code": "uk"},
            auth_date=1_700_000_300,
        )
        user = SimpleNamespace(
            tg_user_id=1003,
            first_name="Taras",
            base_currency="UAH",
            lang="uk",
        )
        registration = SimpleNamespace(user=user, identity=object(), created=False)

        with (
            patch("miniapp.views.validate_telegram_init_data", return_value=identity),
            patch("miniapp.views.bootstrap_telegram_identity", return_value=registration),
            patch("miniapp.views.login_session"),
            patch("miniapp.views.resolve_access", return_value={"mode": "active"}),
            patch("miniapp.views.queue_registration_event") as record_event,
        ):
            response = __import__("miniapp.views", fromlist=["views"]).auth_telegram(request)

        self.assertEqual(response.status_code, 200)
        record_event.assert_not_called()


class PartnerLinkAdminTests(SimpleTestCase):
    def test_partner_links_are_managed_in_admin_with_funnel_metrics(self) -> None:
        self.assertIn(models.PartnerLink, admin_site._registry)
        model_admin = admin_site._registry[models.PartnerLink]

        self.assertTrue(
            {
                "public_link",
                "human_clicks",
                "unique_visitors",
                "first_touch_registrations",
                "last_touch_registrations",
                "registration_rate",
                "onboarding_conversions",
                "first_transaction_conversions",
                "payment_conversions",
            }.issubset(set(model_admin.list_display))
        )
        self.assertFalse(model_admin.has_delete_permission(SimpleNamespace(user=None)))

    def test_partner_links_have_a_primary_admin_navigation_entry(self) -> None:
        menu_items = {
            model_key: label
            for section in PRIMARY_MENU_SECTIONS
            for model_key, label in section.items
        }

        self.assertEqual(menu_items["miniapp.PartnerLink"], "Партнерські посилання")
        self.assertEqual(ADMIN_PAGE_METADATA["miniapp.PartnerLink"]["section"], "Користувачі")
