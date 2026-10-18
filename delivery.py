#!/usr/bin/env python
"""Run the canonical delivery CLI from the repository or its normal outer workspace."""
from pathlib import Path
import subprocess
import sys


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    workspace = Path(__file__).resolve().parent
    root = next((path for path in (workspace, workspace / "code")
                 if (path / "ops/delivery.py").is_file()), None)
    if root is None:
        print("INCOMPLETE: canonical ops/delivery.py is missing from this workspace", file=sys.stderr)
        return 2
    if arguments and not arguments[0].startswith("-") and not any(
        argument == "--root" or argument.startswith("--root=") for argument in arguments
    ):
        arguments[1:1] = ["--root", str(root)]
    return subprocess.call([sys.executable, str(root / "ops/delivery.py"), *arguments], cwd=root)


if __name__ == "__main__":
    raise SystemExit(main())
