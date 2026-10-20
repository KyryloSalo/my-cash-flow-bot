"""Browser acceptance of the actual PartnerLink changelist."""
import json
import os
from datetime import datetime, timedelta
from decimal import Decimal
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo
from playwright.sync_api import sync_playwright


def number(cell):
    value = cell.inner_text().replace('\u00a0', '').replace('\u202f', '').replace(' ', '').replace(',', '.')
    return Decimal(value)


def browser_acceptance(base, output):
    route = base + '/miniapp/partnerlink/'
    checks, screenshots = [], []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={'width': 1440, 'height': 1080})
        errors = []
        page.route('**/*', lambda route: route.continue_() if route.request.url.startswith(base) else route.abort())
        page.on('pageerror', lambda exc: errors.append(str(exc)))
        def login(username):
            page.context.clear_cookies()
            page.goto(base + '/login/')
            page.locator('#id_username').fill(username)
            page.locator('#id_password').fill(os.environ['MCF_ACCEPTANCE_PASSWORD'])
            page.locator('button[type=submit]').click()
            page.wait_for_url(lambda url: '/login/' not in str(url))
        def search(text):
            page.locator('#searchbar').fill(text)
            with page.expect_navigation():
                page.locator('#searchbar').press('Enter')
        login('limited')
        page.goto(route)
        assert page.locator('#partner-period-form').count() == 1
        today = datetime.now(ZoneInfo('Europe/Kyiv')).date()
        for label, start, end in [('Сьогодні', today, today), ('Вчора', today-timedelta(days=1), today-timedelta(days=1)), ('Останній тиждень', today-timedelta(days=6), today), ('Цей місяць', today.replace(day=1), today)]:
            with page.expect_navigation():
                page.get_by_role('link', name=label, exact=True).click()
            assert page.locator('input[type=date][name=period_start]').input_value() == start.isoformat()
            assert page.locator('input[type=date][name=period_end]').input_value() == end.isoformat()
        checks.append('four presets and default this month, Europe/Kyiv')
        search('Partner')
        page.locator('input[type=date][name=period_start]').fill('2026-01-01')
        page.locator('input[type=date][name=period_end]').fill('2026-02-01')
        with page.expect_navigation():
            page.get_by_role('button', name='Застосувати', exact=True).click()
        row = page.locator('.pc-partner').filter(has=page.locator('.pc-link-name', has_text='Browser Partner'))
        expected = {'clicks': 2, 'registrations': 1, 'successful_count': 5, 'successful_amount': 650, 'first_count': 1, 'first_amount': 200, 'repeat_count': 4, 'repeat_amount': 450, 'refund_amount': 200}
        for metric, value in expected.items():
            assert number(row.locator(f'.pc-main [data-metric={metric}]')) == value, metric
        empty = page.locator('.pc-partner').filter(has=page.locator('.pc-link-name', has_text='Empty Partner'))
        assert empty.count() == 1
        assert number(empty.locator('[data-metric=successful_count]').first) == 0
        checks.append('exact seven UAH metrics, all boundaries, later-created inactive empty link retained')
        row.locator('[data-partner-toggle]').click()
        for metric in ('unique_visitors', 'auth', 'last_registrations', 'onboarding', 'first_transaction', 'payment'):
            assert number(row.locator(f'[data-funnel={metric}]')) == 1, metric
        assert row.locator('[data-funnel=registration_rate]').inner_text() == '100.0%'
        checks.append('period funnel counts, unique visitors and conversion reconcile')
        for label in ('За днями', 'За тижнями', 'За місяцями'):
            group = row.locator('details').filter(has=page.locator('summary', has_text=label))
            group.locator('summary').click()
            for metric in ('successful_count', 'successful_amount', 'first_count', 'first_amount', 'repeat_count', 'repeat_amount', 'refund_amount'):
                assert sum(number(cell) for cell in group.locator(f'[data-metric={metric}]').all()) == expected[metric], (label, metric)
            group.locator('summary').click()
        checks.append('inline day/week/month buckets reconcile with row totals')
        for width in (1366, 1440, 390):
            page.set_viewport_size({'width': width, 'height': 1080 if width > 400 else 844})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1'), width
            target = output / f'partner-cabinet-{width}.png'
            page.screenshot(path=str(target), full_page=True)
            screenshots.append(target.name)
        page.set_viewport_size({'width': 1440, 'height': 1080})
        search('')
        page.locator('a[href*="p=2"]').first.click()
        page.wait_for_load_state('networkidle')
        assert parse_qs(urlparse(page.url).query)['period_start'] == ['2026-01-01']
        search('Browser')
        assert page.locator('.pc-partner').count() == 1
        assert page.locator('input[type=date][name=period_end]').input_value() == '2026-02-01'
        checks.append('search and pagination preserve both dates')
        page.goto(route + '?period_start=2026-01-01&period_end=2026-02-01&utm_source__exact=browser')
        assert page.locator('.pc-partner').count() == 1
        page.get_by_role('link', name='Вчора', exact=True).click()
        assert parse_qs(urlparse(page.url).query)['utm_source__exact'] == ['browser']
        checks.append('existing source filter preserved by presets')
        page.goto(route + '?period_start=2027-01-01&period_end=2027-01-02&q=Partner')
        assert page.locator('.pc-partner').count() == 2
        assert all(number(cell) == 0 for cell in page.locator('.pc-main [data-metric]').all())
        page.goto(base + '/partner-payments/?start=2026-01-01&end=2026-02-01')
        assert urlparse(page.url).path == '/miniapp/partnerlink/'
        assert page.locator('a[href="/partner-payments/"]').count() == 0
        checks.append('empty period retains links; old route redirects; no separate navigation')
        login('denied')
        assert page.goto(route).status == 403
        page.context.clear_cookies()
        page.goto(route)
        assert '/login/' in page.url
        assert not errors, errors
        browser.close()
    (output / 'cabinet-acceptance.json').write_text(json.dumps({'status': 'passed', 'checks': checks, 'screenshots': screenshots}, ensure_ascii=False, indent=2), encoding='utf-8')
