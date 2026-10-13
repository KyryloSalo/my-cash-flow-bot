# Android adapter

The Android adapter is an HTTPS-only WebView shell plus a `NotificationListenerService`. Fresh installs are fail-closed: no package IDs are bundled or inferred. The native setup controls provide a reachable path to grant Notification Access and enter a package ID plus canonical bank key only after the user has verified them on-device.

## Build

Prerequisites: JDK 17, Android SDK Platform 35 and build-tools, and Gradle 8.9+ (or generate a wrapper with `gradle wrapper --gradle-version 8.9`). From `mobile/android`:

```sh
gradle testDebugUnitTest
# with Android SDK available
gradle assembleDebug
```

Build output is produced by the Gradle wrapper; physical-device permission, reboot, locked-screen, bank-format, and package-ID evidence remain required before enabling a bank.

## Setup

1. Open the Android app and select **Grant notification access**. The status returns to `granted` only when Android reports this listener enabled.
2. On the device, verify the bank notification package ID and enter it with the reviewed canonical bank key. Save the mapping; it is added to the allowlist and encrypted mapping store.
3. Use **Revoke capture and clear mappings** to remove the scoped credential, mappings, and the allowlist. The status then reports no configured banks and capture remains fail-closed.

### Pairing contract

The authenticated PWA owns pairing start/approval. It creates a short-lived, capture-scoped credential, displays the approval step, and after approval calls the Android-only handoff:

The bridge contract is JSON over `window.AndroidCapture.postMessage`:

```js
window.AndroidCapture.postMessage(JSON.stringify({action: "handoff", credential, connection_id}))
window.AndroidCapture.postMessage(JSON.stringify({action: "revoke", connection_id}))
```

`handoff` carries the short-lived submit credential only from the authenticated PWA to Android; `connection_id` is non-secret and may be retained for synchronization. `revoke` is sent only after the PWA has authenticated `POST /app/api/capture/v1/connections/{connection_id}/revoke`; native shutdown is fail-closed even if that request cannot complete. The native control asks the loaded PWA to perform the same authenticated revoke before clearing local credentials. The credential is never exposed back to JavaScript, URLs, logs, analytics, browser history, or notification text. Pairing must treat a missing/false handoff as failure and offer retry.

A pairing implementation must use authenticated PWA requests for start/approve and must not put the credential in URLs, logs, analytics, browser history, or notification text. The Android adapter does not invent or infer credentials.

No real bank package IDs are documented or preconfigured. A mapping alone does not bypass the listener permission, and an enabled listener with no mapping still captures nothing.

## Security/behavior

- WebView permits only `https://vydno.capital/`; no JavaScript-to-native bridge is exposed.
- Listener rejects packages before parsing. Unknown, OTP, transfer, refund, and income notifications are dropped; incomplete purchases become `unknown` for review.
- Raw notification text is not persisted, logged, or sent. Credentials use Android encrypted preferences.
- Queue IDs derive from the notification key and are stable across updates. Queue TTL is seven days with exponential retry; 409 is terminal conflict and never retried with a new UUID.
- Revoke/kill-switch is represented by 401/403 terminal transport state; the app must clear credentials and stop delivery.

Real-device permission, reboot, locked-screen, bank-format, and package-ID evidence remain required before enabling a bank.
