from __future__ import annotations

import os
from typing import Optional


def env_override(value: Optional[str], env_key: str) -> Optional[str]:
    # This code here lets env vars win when CLI is quiet.
    if value:
        return value
    return os.environ.get(env_key)
