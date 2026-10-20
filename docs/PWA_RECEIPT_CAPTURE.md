# PWA receipt photo intake

Implementation candidate; not a production release record.

## User flow

In Add operation → From photo, choose **Take receipt photo** or **Choose from gallery**. The first uses a native file input with `accept="image/*" capture="environment"`; the second deliberately has no capture attribute. The browser/OS controls the native picker and may use a fallback. No embedded camera stream or new camera-permission service is introduced.

A chosen image is previewed locally using an object URL. Cancellation keeps the previous selection; choosing a replacement clears previous recognition findings. JPEG, PNG, WebP, GIF, HEIC and HEIF files up to 10 MiB are accepted; server-side decoding remains authoritative. When a browser cannot render a format (notably HEIC on some devices), an explanatory placeholder does not block uploading it.

Recognition starts only after pressing Recognize. A green scan beam and registration marks overlay the preview while the recognition promise is pending. There are no fabricated progress percentages or minimum waiting time. Both source inputs and recognition are locked during the request. Success, unrecognized content and errors stop the effect and unlock retry. Reduced-motion users see a static indicator; status changes are announced politely.

The existing recognition API and transaction/batch review remain unchanged: recognizing a photo does **not** confirm or persist a financial operation.

## Local verification

- `node --test admin_service/miniapp/receipt_capture.test.cjs`
- `python ops/receipt_capture_acceptance.py`

Browser acceptance uses production form markup, CSS and functions with a mocked recognition/session boundary. It validates UI request lifecycle, not provider OCR accuracy or physical phone hardware.

## Required real-device checks before claiming mobile certification

On iPhone Safari and home-screen PWA, plus Android Chrome and installed PWA:

- Open From photo; both camera and gallery choices are visible.
- Take a rear-camera photo and accept it; preview appears.
- Cancel camera and gallery; the prior photo remains selected.
- Select JPEG and native HEIC where applicable; rotate portrait/landscape photos.
- Press Recognize; inspect scan animation and readable pending status.
- Check successful recognition, network failure, retry and confirmation review.
- Enable OS Reduce Motion; confirm scan is static.
- Check camera-denied/unavailable fallback via gallery.

Desktop Chromium/WebKit automation is not evidence that a native camera works on a real iPhone or Android handset.
