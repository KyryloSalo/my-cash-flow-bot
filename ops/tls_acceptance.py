#!/usr/bin/env python3
"""Production acceptance of installed controls and temporary public ACME proof."""
import argparse, subprocess, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent
CONTROL = ROOT / "tls_ops.py"
p = argparse.ArgumentParser()
p.add_argument("--production", action="store_true", required=True)
p.add_argument("--probe", action="store_true", help="Compatibility flag; full installed-control acceptance always runs.")
a = p.parse_args()

for command in (("status",), ("acme-check",)):
    result = subprocess.run([sys.executable, str(CONTROL), *command], text=True)
    if result.returncode:
        raise SystemExit(result.returncode)
print("TLS production acceptance passed: status and public ACME proof")
