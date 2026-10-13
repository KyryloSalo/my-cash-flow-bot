# Android adapter

The Android adapter is an HTTPS-only WebView shell plus a `NotificationListenerService`. The bank package allowlist is intentionally empty in source/configuration until verified on a physical device; no package IDs are inferred or claimed.

## Build

Prerequisites: JDK 17, Android SDK Platform 35 and build-tools, and Gradle 8.9+ (or generate a wrapper with `gradle wrapper --gradle-version 8.9`). From `mobile/android`:

```sh
gradle testDebugUnitTest
# with Android SDK available
gradle assembleDebug
```

No APK was produced in this environment: `java` and `gradle` are not installed and `ANDROID_HOME` is unset.

## Security/behavior

- WebView permits only `https://vydno.capital/`; no JavaScript-to-native bridge is exposed.
- Listener rejects packages before parsing. Unknown, OTP, transfer, refund, and income notifications are dropped; incomplete purchases become `unknown` for review.
- Raw notification text is not persisted, logged, or sent. Credentials use Android encrypted preferences.
- Queue IDs derive from the notification key and are stable across updates. Queue TTL is seven days with exponential retry; 409 is terminal conflict and never retried with a new UUID.
- Revoke/kill-switch is represented by 401/403 terminal transport state; the app must clear credentials and stop delivery.

Real-device permission, reboot, locked-screen, bank-format, and package-ID evidence remain required before enabling a bank.
