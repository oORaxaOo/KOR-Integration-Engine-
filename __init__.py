"""Hermes root plugin entry point for the KOR Integration Engine."""
from __future__ import annotations

import sys
from pathlib import Path

_SOURCE_ROOT = Path(__file__).resolve().parent / "src"
_source_text = str(_SOURCE_ROOT)
if _source_text not in sys.path:
    sys.path.insert(0, _source_text)

from kie.hermes_plugin import register  # noqa: E402

__all__ = ["register"]
