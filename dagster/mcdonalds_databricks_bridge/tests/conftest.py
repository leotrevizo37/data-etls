from __future__ import annotations

import sys
from pathlib import Path


TESTS_PATH = Path(__file__).resolve()
BRIDGE_PROJECT_PATH = TESTS_PATH.parents[1]
BRIDGE_SRC_PATH = BRIDGE_PROJECT_PATH / "src"
REPO_DAGSTER_ROOT = BRIDGE_PROJECT_PATH.parents[0]


def _remove_repo_dagster_shadowing() -> None:
    dagster_root = REPO_DAGSTER_ROOT.resolve()

    sanitized_sys_path = []
    for entry in sys.path:
        candidate = Path(entry or ".").resolve()
        if candidate == dagster_root:
            continue
        sanitized_sys_path.append(entry)

    sys.path[:] = sanitized_sys_path

    dagster_module = sys.modules.get("dagster")
    dagster_module_file = getattr(dagster_module, "__file__", "") if dagster_module else ""
    if dagster_module_file:
        try:
            dagster_module_path = Path(dagster_module_file).resolve()
        except OSError:
            dagster_module_path = None

        if dagster_module_path and dagster_root in dagster_module_path.parents:
            for module_name in list(sys.modules):
                if module_name == "dagster" or module_name.startswith("dagster."):
                    sys.modules.pop(module_name, None)


_remove_repo_dagster_shadowing()

if BRIDGE_SRC_PATH.is_dir():
    bridge_src = str(BRIDGE_SRC_PATH)
    if bridge_src not in sys.path:
        sys.path.insert(0, bridge_src)
