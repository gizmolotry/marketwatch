"""Direct entry point for one bounded, explicit-source evidence cycle."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from marketleak.cli_v2 import main


if __name__ == "__main__":
    raise SystemExit(main(["collect-evidence-once", *sys.argv[1:]]))
