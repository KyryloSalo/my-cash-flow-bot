#!/usr/bin/env python3
"""Real-browser mounted receipt component, not authenticated full-PWA acceptance.
Uses shipped HTML/CSS/functions. Session, OCR and editor are explicit test doubles.
Does NOT certify native phone camera, installed-PWA system UI or provider OCR.
"""
import json
import re
import tempfile
from pathlib import Path
from PIL import Image, ImageDraw
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / '.hermes/delivery/20261020_013552_086d02'
SCRATCH = Path.home() / 'AppData/Local/hermes/cache/scratch'


def fixture():
    template = (ROOT / 'admin_service/miniapp/templates/miniapp/index.html').read_text(encoding='utf-8')
    css = (ROOT / 'admin_service/miniapp/static/miniapp/app.css').read_text(encoding='utf-8')
    form = re.search(r'<form\b[^>]*id="aiImageForm"[\s\S]*?</form>', template).group()
    launch = re.search(r'<button\b[^>]*id="aiImageLaunchBtn"[\s\S]*?</button>', template).group()
    names = ['aiImageElements', 'aiVoiceElements', 'aiTextElements', 'setAiIntakeMode',
             'setupAiIntakeChooser', 'setAiImageStatus', 'setAiImageBusy', 'syncAiImageFileName',
             'isValidAiImageFile', 'selectAiImageFile', 'renderAiImageFindings', 'parseAiImage', 'setupAiImageForm']
    functions = []
    for name in names:
        start = template.index('      function ' + name + '(')
        end = template.index('\n      }', start) + len('\n      }')
        functions.append(template[start:end])
    state = re.search(r'const aiImageCaptureState = [^;]+;', template).group()
    copy = dict(reversed(re.findall(r'^\s*(aiImage\w+): "([^"\n]*)"', template, re.M)))
    setup = """
      const transactionState = {busy:false}, aiImageBatchState = {};
      const aiImageParseUrl = '/mock-recognition';
      window.probe = {requests:0, editorCalls:0, batchCalls:0, confirmCalls:0};
      function tr(key) { return copy[key] || key; }
      function trf(key, values) { return tr(key).replace('{value}', values.value); }
      function transactionErrorMessage(error) { return error.message; }
      function ensureSession() { return Promise.resolve(); }
      function postFormData(url, data) {
        probe.requests++; probe.uploadName = data.get('image').name;
        return new Promise((resolve,reject) => { probe.resolve=resolve; probe.reject=reject; });
      }
      function loadTransactionOptions() { return Promise.resolve(); }
      function applyAiTextSuggestion() { probe.editorCalls++; return true; }
      function setAiImageBatch() { probe.batchCalls++; }
      window.fetch = function() { probe.confirmCalls++; throw new Error('Unexpected network call'); };
    """
    return ("<!doctype html><html lang='uk'><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>"
            f"<style>{css}</style><body class='calm-fintech-v2'><main class='tx-card' style='margin:12px'>{launch}{form}</main>"
            f"<script>const copy={json.dumps(copy)};\n{state}\n{setup}\n" + '\n'.join(functions) +
            '\nsetupAiIntakeChooser(); setupAiImageForm();</script></body></html>')


def run_case(browser, html, engine, width, height, reduced, photo, replacement):
    context = browser.new_context(viewport={'width':width, 'height':height}, reduced_motion='reduce' if reduced else 'no-preference')
    page = context.new_page()
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    try:
        page.set_content(html)
        assert not errors, errors
        assert page.locator('#aiImageForm').is_hidden()
        page.locator('#aiImageLaunchBtn').click()
        assert page.locator('#aiImageForm').is_visible()
        camera, gallery = page.locator('#aiImageCameraInput'), page.locator('#aiImageInput')
        assert camera.get_attribute('capture') == 'environment'
        assert gallery.get_attribute('capture') is None
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'horizontal overflow'
        for selector in ['label[for="aiImageCameraInput"]', 'label[for="aiImageInput"]']:
            with page.expect_file_chooser() as chooser:
                page.locator(selector).click()
            chooser.value.set_files(str(photo))
            page.wait_for_function("document.querySelector('#aiImagePreview').naturalWidth > 0")
        old_src = page.locator('#aiImagePreview').get_attribute('src')
        camera.set_input_files([])
        assert page.locator('#aiImagePreview').get_attribute('src') == old_src
        page.locator('#aiImageParseBtn').click()
        page.wait_for_function('probe.requests === 1')
        review, beam = page.locator('#aiImageReview'), page.locator('.ai-image-scan-beam')
        assert review.get_attribute('aria-busy') == 'true'
        assert review.evaluate("el => el.classList.contains('is-scanning')")
        assert camera.is_disabled() and gallery.is_disabled()
        assert beam.evaluate('el => getComputedStyle(el).animationName') == ('none' if reduced else 'receipt-scan')
        if not reduced:
            before = beam.evaluate('el => getComputedStyle(el).transform')
            page.wait_for_timeout(200)
            assert before != beam.evaluate('el => getComputedStyle(el).transform'), 'beam must move'
        if engine == 'chromium' and width == 390 and not reduced:
            EVIDENCE.mkdir(parents=True, exist_ok=True)
            page.locator('#aiImageForm').screenshot(path=str(EVIDENCE / 'scan-mobile.png'))
        page.locator('#aiImageForm').evaluate("el => el.dispatchEvent(new Event('submit', {cancelable:true}))")
        assert page.evaluate('probe.requests') == 1
        assert page.evaluate('probe.uploadName') == photo.name
        page.evaluate("probe.resolve({suggestion:{mode:'transaction'}})")
        page.wait_for_function("!document.querySelector('#aiImageParseBtn').disabled")
        assert page.evaluate('probe.editorCalls') == 1
        assert review.get_attribute('aria-busy') == 'false'
        assert not review.evaluate("el => el.classList.contains('is-scanning')")
        page.locator('#aiImageFindings').evaluate("el => el.textContent='Prior result'")
        gallery.set_input_files(str(replacement))
        assert page.locator('#aiImageFindings').inner_text() == ''
        for bad in [{'name':'invalid.svg','mimeType':'image/svg+xml','buffer':b'<svg/>'},
                    {'name':'large.png','mimeType':'image/png','buffer':b'0'*(10*1024*1024+1)}]:
            gallery.set_input_files(bad)
            assert page.locator('#aiImageStatus').is_visible()
            assert page.locator('#aiImageFileName').inner_text() == replacement.name
        camera.set_input_files({'name':'receipt.heic','mimeType':'image/heic','buffer':b'HEIC placeholder'})
        page.wait_for_function("!document.querySelector('#aiImagePreviewHint').hidden")
        page.locator('#aiImageParseBtn').click()
        page.wait_for_function('probe.requests === 2')
        assert review.get_attribute('aria-busy') == 'true'
        page.evaluate("probe.reject(new Error('Mock offline failure'))")
        page.wait_for_function("!document.querySelector('#aiImageParseBtn').disabled")
        assert review.get_attribute('aria-busy') == 'false'
        assert 'Mock offline failure' in page.locator('#aiImageStatus').inner_text()
        assert not camera.is_disabled() and not gallery.is_disabled()
        gallery.set_input_files(str(photo))
        for count, suggestion in [(3, {'mode':'batch', 'items':[]}), (4, {})]:
            page.locator('#aiImageParseBtn').click()
            page.wait_for_function(f'probe.requests === {count}')
            page.evaluate('(suggestion) => probe.resolve({suggestion})', suggestion)
            page.wait_for_function("!document.querySelector('#aiImageParseBtn').disabled")
            assert review.get_attribute('aria-busy') == 'false'
        assert page.evaluate('probe.batchCalls') == 1
        assert page.evaluate('probe.confirmCalls') == 0
        assert not errors, errors
    except Exception:
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(EVIDENCE / f'failure-{engine}-{width}-{reduced}.png'))
        print('Browser errors:', errors)
        raise
    finally:
        context.close()


def main():
    html = fixture()
    SCRATCH.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='receipt-acceptance-', dir=SCRATCH) as temp:
        photo, replacement = Path(temp)/'receipt.png', Path(temp)/'replacement.png'
        for image_path in [photo, replacement]:
            image = Image.new('RGB', (420, 700), '#fcfaf5')
            draw = ImageDraw.Draw(image)
            for y, text in [(45,'TEST RECEIPT / UI FIXTURE'), (140,'Coffee       65.00'), (195,'Lunch       185.00'), (290,'TOTAL       250.00 UAH'), (420,'Synthetic data - not OCR evidence')]:
                draw.text((45,y), text, fill='#243f37')
            image.save(image_path)
        failures, completed = [], 0
        with sync_playwright() as playwright:
            for engine in ['chromium','webkit']:
                try:
                    browser = getattr(playwright, engine).launch()
                except Exception as exc:
                    failures.append(f'{engine} unavailable: {exc}')
                    continue
                try:
                    for width,height in [(390,844),(360,800)]:
                        for reduced in [False,True]:
                            label = f'{engine} {width}x{height} reduced_motion={reduced}'
                            try:
                                run_case(browser,html,engine,width,height,reduced,photo,replacement)
                                completed += 1
                                print(f'PASS {label}')
                            except Exception as exc:
                                failures.append(f'{label}: {exc}')
                                print(f'FAIL {failures[-1]}')
                finally:
                    browser.close()
        for failure in failures:
            print(failure)
        print(f'Completed cases: {completed}/8. Mocked session/OCR/editor; native devices NOT certified.')
        return 1 if failures or completed != 8 else 0


if __name__ == '__main__':
    raise SystemExit(main())
