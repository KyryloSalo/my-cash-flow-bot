from datetime import date
from urllib.parse import parse_qs, urlsplit
import re
from html import unescape
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from miniapp.models import PartnerLink


class PartnerCabinetAdminTests(TestCase):
    def setUp(self):
        self.staff = get_user_model().objects.create_user('cabinet-viewer', is_staff=True)
        self.staff.user_permissions.add(Permission.objects.get(content_type__app_label='miniapp', codename='view_partnerlink'))
        self.client.force_login(self.staff)
        self.link = PartnerLink.objects.create(name='Visible partner', code='visible-partner', utm_source='visible')
        self.url = reverse('admin:miniapp_partnerlink_changelist')

    def test_existing_partner_links_screen_has_financial_columns_and_range(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        for label in ('Успішних оплат за період', 'Сума оплат за період', 'Кількість перших оплат', 'Сума перших оплат', 'Кількість повторних оплат', 'Сума повторних оплат', 'Сума повернень'):
            self.assertContains(response, label)
        self.assertContains(response, 'id="partner-period-form"')
        self.assertContains(response, 'type="date"', count=2)
        self.assertContains(response, 'Сьогодні')
        self.assertContains(response, 'Вчора')
        self.assertContains(response, 'Останній тиждень')
        self.assertContains(response, 'Цей місяць')
        self.assertNotContains(response, "Платежі партнерів")
        today = timezone.now().astimezone(ZoneInfo('Europe/Kyiv')).date()
        self.assertEqual(response.context_data['period_start'], today.replace(day=1))
        self.assertEqual(response.context_data['period_end'], today)

    def test_old_report_redirects_without_extra_payment_permission(self):
        user = get_user_model().objects.create_user("partner-viewer", is_staff=True)
        user.user_permissions.add(Permission.objects.get(codename="view_partnerlink"))
        self.client.force_login(user)
        response = self.client.get(reverse("admin:partner_payments"), {"date_from": "2026-09-01", "date_to": "2026-09-07"})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, self.url + "?period_start=2026-09-01&period_end=2026-09-07")

    def test_partner_only_permission_reads_financial_columns(self):
        user = get_user_model().objects.create_user("links-only", is_staff=True)
        user.user_permissions.add(Permission.objects.get(codename="view_partnerlink"))
        self.client.force_login(user)
        self.assertContains(self.client.get(self.url), "Успішних оплат за період")

    def test_invalid_period_is_visible_and_not_silently_replaced(self):
        response = self.client.get(self.url, {"period_start": "2026-10-20", "period_end": "2026-10-01"})
        self.assertContains(response, "Дата початку має бути не пізнішою за кінець")
        self.assertNotContains(response, "· включно")

    def test_edit_link_preserves_period_using_standard_admin_filters(self):
        response = self.client.get(self.url, {"period_start": "2026-01-01", "period_end": "2026-02-01", "q": "Visible"})
        edit_path = reverse('admin:miniapp_partnerlink_change', args=[self.link.pk])
        hrefs = re.findall(r'href="([^"]+)"', response.content.decode())
        edit_url = next(unescape(href) for href in hrefs if href.startswith(edit_path))
        preserved = parse_qs(urlsplit(edit_url).query)['_changelist_filters'][0]
        self.assertEqual(parse_qs(preserved)['period_start'], ['2026-01-01'])
        self.assertEqual(parse_qs(preserved)['period_end'], ['2026-02-01'])
