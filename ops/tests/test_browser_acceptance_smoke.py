from pathlib import Path


SMOKE = Path(__file__).parents[1] / "browser_acceptance_smoke.py"


def test_smoke_uses_explicit_isolated_postgres_and_never_inherited_database_url():
    source = SMOKE.read_text(encoding="utf-8")
    assert 'env.pop("DATABASE_URL", None)' in source
    assert '"DATABASE_URL": f"postgresql://' in source
    assert '"-p", "127.0.0.1::5432"' in source
    assert '"docker", "rm", "-f", container' in source


def test_smoke_targets_real_app_route_and_user_visible_onboarding():
    source = SMOKE.read_text(encoding="utf-8")
    assert 'base_url = f"http://127.0.0.1:{port}/app"' in source
    assert 'page.locator("#onboardingShell").wait_for(state="visible"' in source
    assert 'page.locator(\'nav[aria-label="Навігація"] button\').count() >= 1' in source
    assert 'page.request.get(base_url + "/manifest.webmanifest").ok' in source
    assert 'page.request.get(base_url + "/sw.js").ok' in source
