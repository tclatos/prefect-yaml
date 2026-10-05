"""Pytest configuration ensuring localhost bypasses proxy."""

import os

# Ensure localhost and 127.0.0.1 bypass any HTTP proxy
current_no_proxy = os.environ.get("NO_PROXY", "")
hosts = ["127.0.0.1", "localhost", "*********", "*"]
for h in hosts:
    if h not in current_no_proxy:
        current_no_proxy = f"{current_no_proxy},{h}" if current_no_proxy else h
os.environ["NO_PROXY"] = current_no_proxy
os.environ["no_proxy"] = current_no_proxy
