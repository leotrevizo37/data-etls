import sys
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
PROJECT_PACKAGE = PROJECT_SRC / "emerson"

if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

loaded_emerson = sys.modules.get("emerson")
if loaded_emerson is not None and hasattr(loaded_emerson, "__path__"):
    package_paths = list(loaded_emerson.__path__)
    if str(PROJECT_PACKAGE) not in package_paths:
        loaded_emerson.__path__.insert(0, str(PROJECT_PACKAGE))
