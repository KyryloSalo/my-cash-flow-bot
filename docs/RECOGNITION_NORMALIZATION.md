# Recognition review and evidence contract

Status: release candidate; this document does not assert production activation.

## User flow

PWA is the primary interface: image upload → editable recognition draft →
explicit review of unresolved fields → explicit financial confirmation → write.
Telegram remains a supported entry/notification surface, not a replacement for
PWA acceptance.

The extraction response is untrusted input, not a financial transaction. Preserve
supported fields and source evidence when another field is uncertain. Do not
replace a missing date with today, inherit a row currency from the document, or
rewrite all row currencies after a document-level currency choice. Keep uncertain
and non-bookable rows available as evidence instead of silently discarding them.

## Deterministic safeguards

- Money is parsed as Decimal; ambiguous formats and mismatching amount evidence
  cannot silently become confirmed values.
- `amount_text`, `date_text`, and `currency_text` are source fragments, not
  normalized substitutes. A missing year stays unresolved; no current-year or
  weekday inference is permitted.
- A booked local amount is paired with its local currency, not the original
  foreign amount/currency printed elsewhere on a bank receipt.
- Pending, declined, refund, own-account-transfer and contradictory operation
  evidence remain non-bookable through expense confirmation. A visible income
  must never be retyped as an expense because its status is unknown.
- Receipt line arithmetic and total mismatches remain review findings.
- Review resolution is server-owned and bound to the source/draft fields.
  A client flag such as `needs_review=false` is not authorization.
- Editing and acknowledgement never replace the final confirmation step.
  Confirmation retries must not duplicate a financial write.

## Model configuration

The candidate vision default is `gpt-5.4-mini`. `OPENAI_VISION_MODEL` and
`OPENAI_VISION_FALLBACK_MODEL` remain explicit operator overrides. Text and
speech models are unchanged. A deployment with an existing vision-model
override does **not** automatically use the new default: release verification
must read back only the effective non-secret model names and check they match
the model used in the accepted real-image run. Never print API keys or full
container environments to perform that check.

Changing a model or prompt invalidates prior provider-quality evidence.
Paid calls must use already authorized access and masked images. Private images,
raw provider responses and per-user baselines stay outside Git and CI artifacts.

## Verification layers

1. Unit/backend/JavaScript regressions: parsing, evidence preservation, review
   binding, tamper/stale-state rejection, confirmation and retry guards.
2. Disposable PostgreSQL/Redis plus real Chromium/PWA acceptance: actual editing,
   acknowledgement, server confirmation and one persisted transaction.
3. Real-image provider checks: bind captured input bytes to the image checksum,
   record effective response model and completion status, and compare with
   independently inspected visible facts. Matching unsigned amount/currency
   occurrences alone does not prove row identity, operation type or date.
4. A captured-provider replay through PWA proves that captured response reaches
   the confirmation/write flow; report it separately from a fresh online call.
5. GitHub required `validate`, independent review and protected-main integration
   precede exact-SHA production validation/promotion. Production Environment
   approval and post-deploy readback remain required.

The named PWA smoke route and recognition acceptance use isolated local services
without copying production credentials. A successful Docker build is not a
successful runtime startup; a missing local `.env` must be reported as a blocked
Compose runtime check, not relabeled as a pass.
