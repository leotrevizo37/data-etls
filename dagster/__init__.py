from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Sequence


class AssetCheckSeverity(Enum):
    WARN = "WARN"
    ERROR = "ERROR"


@dataclass(frozen=True)
class AssetKey:
    path: Sequence[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", tuple(self.path))


@dataclass
class AssetCheckResult:
    passed: bool
    severity: AssetCheckSeverity = AssetCheckSeverity.ERROR
    metadata: Mapping[str, Any] = field(default_factory=dict)


def asset_check(
    *_args: Any,
    **_kwargs: Any,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        return fn

    return decorator
