from __future__ import annotations

from unittest.mock import patch

from django.template.loader import render_to_string
from django.test import SimpleTestCase

from dashboard.funnel import build_pwa_funnel_report, build_pwa_funnel_report_from_rows, build_stage_rows
from dashboard.tests_investor_metrics import synthetic_dashboard


class PwaFunnelStageTests(SimpleTestCase):
    def test_stage_counts_are_distinct_sessions_in_contract_order(self) -> None:
        rows = build_stage_rows(
            cohort_session_ids={"a", "b", "c"},
            stage_session_ids={
                "landing_view": {"a", "b", "c"},
                "landing_primary_cta_click": {"a", "b"},
                "auth_success": {"a"},
                "payment_success": {"a"},
            },
        )

        self.assertEqual(
            [row["event_name"] for row in rows[:4]],
            [
                "landing_view",
                "landing_primary_cta_click",
                "auth_success",
                "onboarding_confirmed",
            ],
        )
        self.assertEqual([row["value"] for row in rows[:4]], [3, 2, 1, 0])
        self.assertEqual(rows[1]["overall_percent"], 66.7)
        self.assertEqual(rows[2]["previous_percent"], 50.0)

    def test_default_report_excludes_internal_and_automation_but_labels_anonymous(self) -> None:
        sessions = [
            {"id": "real", "user_id": 100, "is_test_user": False, "automation_status": "unknown"},
            {"id": "test", "user_id": 101, "is_test_user": True, "automation_status": "unknown"},
            {"id": "operator", "user_id": 102, "is_test_user": False, "automation_status": "verified_human"},
            {"id": "bot", "user_id": None, "is_test_user": False, "automation_status": "automated"},
            {"id": "anonymous", "user_id": None, "is_test_user": False, "automation_status": "unknown"},
        ]
        events = [
            {"acquisition_session_id": item["id"], "event_name": "landing_view"}
            for item in sessions
        ]

        report = build_pwa_funnel_report_from_rows(
            session_rows=sessions,
            event_rows=events,
            include_test_users=False,
            excluded_telegram_ids={102},
        )

        self.assertEqual(report["cohort_sessions"], 2)
        self.assertEqual(report["anonymous_unclassified"], 1)
        self.assertEqual(report["excluded_internal"], 2)
        self.assertEqual(report["excluded_automation"], 1)
        self.assertEqual(report["stages"][0]["value"], 2)

    @patch("dashboard.funnel.funnel_excluded_telegram_ids", return_value=set())
    @patch("dashboard.funnel.FunnelEvent")
    @patch("dashboard.funnel.AcquisitionSession")
    def test_orm_report_projects_only_privacy_safe_dimensions(
        self,
        acquisition_model,
        event_model,
        _excluded_ids,
    ) -> None:
        acquisition_values = acquisition_model.objects.filter.return_value.values
        acquisition_values.return_value = [
            {
                "id": "real",
                "user_id": 100,
                "user__admin_state__is_test_user": False,
                "automation_status": "unknown",
            }
        ]
        event_values = event_model.objects.filter.return_value.values
        event_values.return_value = [
            {"acquisition_session_id": "real", "event_name": "landing_view"}
        ]

        report = build_pwa_funnel_report(
            start_at="2026-09-01",
            end_at="2026-10-01",
            include_test_users=False,
        )

        self.assertEqual(report["stages"][0]["value"], 1)
        acquisition_values.assert_called_once_with(
            "id",
            "user_id",
            "user__admin_state__is_test_user",
            "automation_status",
        )
        event_values.assert_called_once_with("acquisition_session_id", "event_name")

    @patch(
        "dashboard.services.build_pwa_funnel_report",
        return_value={"cohort_sessions": 1, "stages": []},
    )
    def test_dashboard_context_keeps_pwa_funnel_separate(self, build_report) -> None:
        context = synthetic_dashboard()

        self.assertEqual(context["pwa_funnel"]["cohort_sessions"], 1)
        self.assertIn("funnel_stages", context)
        build_report.assert_called_once()

    def test_dashboard_template_renders_privacy_safe_pwa_funnel(self) -> None:
        html = render_to_string(
            "dashboard/index.html",
            {
                "pwa_funnel": {
                    "available": True,
                    "cohort_sessions": 3,
                    "anonymous_unclassified": 1,
                    "excluded_internal": 2,
                    "excluded_automation": 4,
                    "stages": [
                        {
                            "label": "Landing view",
                            "value": 3,
                            "overall_percent": 100.0,
                            "previous_percent": 100.0,
                        }
                    ],
                }
            },
        )

        self.assertIn("PWA-воронка залучення", html)
        self.assertIn("Анонімні / некласифіковані: 1", html)
        self.assertIn("Landing view", html)
        self.assertNotIn("payment.raw_payload", html)
