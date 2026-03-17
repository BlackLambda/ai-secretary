"""End-to-end tests for the Flask server.

These tests start the server in a subprocess and verify key HTTP endpoints.
Run only when the full environment (Python deps, config) is available.

Usage:
    pytest tests/e2e/test_server_health.py
"""

import subprocess
import sys
import time
from pathlib import Path

import pytest

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

BASE_URL = "http://localhost:5001"
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture(scope="module")
def server():
    """Start server on port 5001 for the test session, then shut it down."""
    if not HAS_REQUESTS:
        pytest.skip("requests package not installed")

    proc = subprocess.Popen(
        [sys.executable, str(REPO_ROOT / "server_react.py"), "--port", "5001"],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server to be ready
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE_URL}/api/status", timeout=2)
            if r.status_code < 500:
                break
        except Exception:
            time.sleep(0.5)
    else:
        proc.terminate()
        pytest.fail("Server did not start in time")

    yield proc

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.mark.e2e
def test_status_endpoint(server):
    r = requests.get(f"{BASE_URL}/api/status", timeout=5)
    assert r.status_code == 200
    data = r.json()
    assert "status" in data or "ok" in str(data).lower()


@pytest.mark.e2e
def test_config_endpoint(server):
    r = requests.get(f"{BASE_URL}/api/config", timeout=5)
    assert r.status_code in (200, 401, 403)


@pytest.mark.e2e
def test_azure_status_endpoint(server):
    r = requests.get(f"{BASE_URL}/api/azure/status", timeout=5)
    assert r.status_code == 200
    data = r.json()
    assert "logged_in" in data


@pytest.mark.e2e
def test_copilot_status_endpoint(server):
    r = requests.get(f"{BASE_URL}/api/copilot/status", timeout=5)
    assert r.status_code == 200
    data = r.json()
    assert "logged_in" in data
