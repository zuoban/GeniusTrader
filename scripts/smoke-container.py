#!/usr/bin/env python3
"""Verify a container with a fresh anonymous volume, never the user's database."""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def request(base, path, data=None):
    payload = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(
        base + path,
        data=payload,
        headers={"Content-Type": "application/json", "X-Genius-Request": "1"},
    )
    # Use loopback directly even when the host has an HTTP proxy configured.
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        req, timeout=5
    ) as response:
        return response.read()


def ready(base):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        try:
            return json.loads(request(base, "/api/state"))
        except (OSError, urllib.error.URLError):
            time.sleep(0.5)
    raise RuntimeError("Container API did not become ready within 45 seconds")


def main(image):
    name = "genius-trader-smoke-" + uuid.uuid4().hex[:12]
    try:
        help_text = docker("run", "--rm", image, "--help")
        assert "--bind" in help_text, help_text
        docker("run", "--detach", "--name", name, "--publish", "127.0.0.1::8787", image)
        inspect = json.loads(docker("inspect", name))[0]
        assert inspect["Config"]["User"] == "10001:10001"
        address = docker("port", name, "8787/tcp")
        assert address.startswith("127.0.0.1:"), address
        base = "http://" + address
        state = ready(base)
        assert state["mode"] == "paper" and state["paused"] is True, state
        assert state["auth"]["initialized"] is False, state["auth"]
        assert b"Genius" in request(base, "/")
        assert b"/api/settings/jev/test" in request(base, "/app.js")
        password = "Container-smoke-" + uuid.uuid4().hex
        initialized = json.loads(
            request(base, "/api/auth/setup", {"password": password, "confirm_password": password})
        )
        assert initialized["initialized"] is True, initialized
        permissions = docker("exec", name, "stat", "-c", "%u:%a", "/app/data/trader.sqlite")
        assert permissions == "10001:600", permissions
        docker("stop", "--time", "15", name)
        stopped = json.loads(docker("inspect", name))[0]
        assert stopped["State"]["ExitCode"] == 0, stopped["State"]
        docker("start", name)
        restarted = ready(base)
        assert restarted["auth"]["initialized"] is True, restarted["auth"]
        assert restarted["mode"] == "paper" and restarted["paused"] is True, restarted
        print("Container verified: UI/API, non-root SQLite permissions, graceful stop, persistence and paused paper mode")
    except Exception:
        subprocess.run(["docker", "logs", "--tail", "50", name], check=False)
        raise
    finally:
        subprocess.run(["docker", "rm", "--force", "--volumes", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 scripts/smoke-container.py IMAGE")
    main(sys.argv[1])
