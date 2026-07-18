"""Convenience entry point for one frozen shadow cycle."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from marketleak.cli_v2 import main


if __name__ == "__main__":
    raise SystemExit(main(["shadow-prospective", *sys.argv[1:]]))
