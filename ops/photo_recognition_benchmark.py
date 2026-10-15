"""Repository CLI wrapper for the benchmark shipped with the bot image."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bot"))
from photo_recognition_benchmark import main, run_benchmark

if __name__ == "__main__":
    sys.exit(main())
