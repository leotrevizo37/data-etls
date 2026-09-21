import importlib
import sys
from pathlib import Path


for user_code in (
    "bafar",
    "bafar_poc_gabinete",
    "carlsjr",
    "emerson",
    "valledelencino",
    "alerting",
):
    sys.path.insert(0, str(Path(__file__).resolve().parent / user_code / "src"))
    importlib.import_module(user_code)
