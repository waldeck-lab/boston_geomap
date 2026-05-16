# File boston_geomap/server/auth_client.py

from __future__ import annotations

import os
from functools import wraps
from typing import Any

import requests
from flask import request, jsonify, g


AUTH_CLIENT_KEY = os.environ.get("AUTH_CLIENT_KEY", "geomap")

WCR_INTROSPECT_URL = os.environ.get(
    "WCR_INTROSPECT_URL",
    "http://127.0.0.1:5000/api/auth/introspect",
)

WCR_INTERNAL_AUTH_SECRET = os.environ.get("WCR_INTERNAL_AUTH_SECRET", "")


def introspect_current_request() -> dict[str, Any]:
    if not WCR_INTERNAL_AUTH_SECRET:
        raise RuntimeError("WCR_INTERNAL_AUTH_SECRET not configured")

    r = requests.get(
        WCR_INTROSPECT_URL,
        params={"client": AUTH_CLIENT_KEY},
        cookies=request.cookies,
        headers={"X-Internal-Auth": WCR_INTERNAL_AUTH_SECRET},
        timeout=2.0,
    )

    if r.status_code != 200:
        return {
            "authenticated": False,
            "error": f"WCR introspection failed: HTTP {r.status_code}",
        }

    return r.json()


def has_grant(auth: dict[str, Any], grant_name: str) -> bool:
    user = auth.get("user") or {}
    groups = set(auth.get("groups") or [])
    grants = set(auth.get("grants") or [])

    return (
        grant_name in grants
        or "admin" in grants
        or "admin" in groups
        or bool(user.get("is_admin"))
    )


def require_grant(grant_name: str):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                auth = introspect_current_request()
            except Exception as exc:
                return jsonify({
                    "ok": False,
                    "error": "auth authority unavailable",
                    "detail": str(exc),
                }), 503

            if not auth.get("authenticated"):
                return jsonify({
                    "ok": False,
                    "error": "authentication required",
                }), 401

            if not has_grant(auth, grant_name):
                return jsonify({
                    "ok": False,
                    "error": "permission denied",
                    "client": AUTH_CLIENT_KEY,
                    "grant": grant_name,
                }), 403

            g.user = auth.get("user")
            g.groups = auth.get("groups") or []
            g.grants = auth.get("grants") or []

            return fn(*args, **kwargs)

        return wrapper

    return deco
