# Photo recognition benchmark fixture

This directory contains an **empty example manifest only**. It is not a release
suite and contains no real photo corpus or accuracy labels. A populated,
versioned manifest must provide each case's image SHA-256, stored extraction
passes, and expected reconciliation include/exclude findings.

Run from the repository root:

```sh
python -m ops.photo_recognition_benchmark bot/tests/fixtures/photo_recognition/manifest.json
```

The runner reports accuracy as **not computed** and exits nonzero when required
corpus fixtures are missing.
