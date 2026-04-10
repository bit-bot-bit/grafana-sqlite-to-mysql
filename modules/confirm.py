from __future__ import annotations

from collections.abc import Iterable


def _format_value(value) -> str:
    if value is None:
        return "<unset>"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def prompt_for_config_confirmation(
    title: str,
    config_path: str | None,
    items: Iterable[tuple[str, object]],
) -> None:
    print(title)
    if config_path:
        print(f"Config file: {config_path}")
    for key, value in items:
        print(f"  {key}: {_format_value(value)}")
    reply = input("Proceed with these settings? [y/N]: ").strip().lower()
    if reply not in {"y", "yes"}:
        raise RuntimeError("Aborted by user during config confirmation.")
