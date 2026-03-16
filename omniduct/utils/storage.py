from __future__ import annotations

import os


def ensure_path_exists(path: str) -> str:
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        os.makedirs(path)
    return path
