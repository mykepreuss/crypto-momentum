from __future__ import annotations

import os

# Avoid starting background jobs / external connections during unit tests.
os.environ.setdefault("APP_ENV", "test")

