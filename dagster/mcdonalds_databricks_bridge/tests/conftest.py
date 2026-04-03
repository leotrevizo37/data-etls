from __future__ import annotations

import sys
from pathlib import Path


BRIDGE_SRC_PATH = Path(__file__).resolve().parents[1] / "src"

if BRIDGE_SRC_PATH.is_dir():
    bridge_src = BRIDGE_SRC_PATH.as_posix()
    if bridge_src not in sys.path:
        sys.path.insert(0, bridge_src)
