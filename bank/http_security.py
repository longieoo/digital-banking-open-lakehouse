"""Optional HTTP Bearer authentication and browser-origin checks.

This module does not terminate TLS. Shared deployments must supply HTTPS at a
trusted reverse proxy and configure allowed hosts explicitly. Tokens never belong
in query strings or browser persistent storage.
"""
from __future__ import annotations

from datetime import datetime
import hmac
import ipaddress
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit


SECURITY_HEADERS = {
    'Cache-Control': 'no-store',
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer',
    'X-Frame-Options': 'DENY',
    'Cross-Origin-Resource-Policy': 'same-origin',
    'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self'; "
                             "img-src 'self'; connect-src 'self'; object-src 'none'; "
                             "frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
}


def _hostname(value):
    if not isinstance(value, str) or len(value) > 253 or any(c in value for c in '\r\n/\\@?#'):
        raise ValueError('Invalid allowed host')
    value = value.lower()
    try:
        return str(ipaddress.ip_address(value.strip('[]')))
    except ValueError:
        if not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', value) or '..' in value:
            raise ValueError('Invalid allowed host') from None
    return value


class TokenAuth:
    """Stateless auth. Mounted secret rotation takes effect on the next request."""
    def __init__(self, token=None, token_file=None, expires_at=None, allowed_hosts=None, clock=None):
        if token and token_file:
            raise ValueError('Choose one HTTP token source')
        self.token, self.token_file = token, Path(token_file) if token_file else None
        self.clock = clock or time.time
        self.expires_at = None
        if expires_at:
            parsed = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                raise ValueError('Token expiration requires a timezone')
            self.expires_at = parsed.timestamp()
        self.allowed_hosts = frozenset(_hostname(host.strip()) for host in
            (allowed_hosts if allowed_hosts is not None else ['localhost', '127.0.0.1', '::1']))
        if not self.allowed_hosts:
            raise ValueError('HTTP allowed hosts must not be empty')
        if self.enabled:
            self.secret()  # Invalid configuration must not silently become anonymous.

    @property
    def enabled(self):
        return self.token_file is not None or bool(self.token)

    @classmethod
    def from_env(cls, require=False):
        hosts = os.environ.get('BANK_HTTP_ALLOWED_HOSTS')
        auth = cls(token=os.environ.get('BANK_HTTP_TOKEN') or None,
            token_file=os.environ.get('BANK_HTTP_TOKEN_FILE') or None,
            expires_at=os.environ.get('BANK_HTTP_TOKEN_EXPIRES_AT') or None,
            allowed_hosts=hosts.split(',') if hosts is not None else None)
        if require and (not auth.enabled or auth.expired):
            raise ValueError('Valid HTTP authentication is required')
        return auth

    @property
    def expired(self):
        return self.expires_at is not None and self.clock() >= self.expires_at

    def secret(self):
        token = self.token
        if self.token_file is not None:
            try:
                if not self.token_file.is_file():
                    raise ValueError('HTTP authentication configuration unavailable')
                with self.token_file.open('rb') as handle:
                    content = handle.read(1025)
                token = content.decode('ascii').rstrip('\r\n')
            except (OSError, UnicodeError):
                raise ValueError('HTTP authentication configuration unavailable') from None
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9._~+/=-]{32,512}', token):
            raise ValueError('HTTP token must contain 32 to 512 ASCII token characters')
        return token

    def request_status(self, headers, protected=True):
        """Return 200, 400, 401, 403 or 503 without exposing secret/config details."""
        hosts = headers.get_all('Host', [])
        if len(hosts) != 1 or len(hosts[0]) > 300:
            return 400
        try:
            host = urlsplit('//'+hosts[0])
            if host.username or host.password or host.path or host.query or host.fragment:
                return 400
            if _hostname(host.hostname) not in self.allowed_hosts:
                return 403
            host_port = host.port  # Validate malformed/oversized ports.
        except (ValueError, TypeError):
            return 400
        origin_values = headers.get_all('Origin', [])
        if len(origin_values) > 1:
            return 403
        if origin_values:
            try:
                origin = urlsplit(origin_values[0])
                if origin.scheme not in {'http', 'https'} or origin.username or origin.password:
                    return 403
                if origin.path or origin.query or origin.fragment or origin.hostname != host.hostname:
                    return 403
                default = 443 if origin.scheme == 'https' else 80
                if (origin.port or default) != (host_port or default):
                    return 403
            except ValueError:
                return 403
        if protected and headers.get('Sec-Fetch-Site', '').lower() == 'cross-site':
            return 403
        if not protected or not self.enabled:
            return 200
        values = headers.get_all('Authorization', [])
        if len(values) != 1 or len(values[0]) > 1024 or self.expired:
            return 401
        parts = values[0].split()
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            return 401
        try:
            expected = self.secret().encode('ascii')
            received = parts[1].encode('ascii')
        except UnicodeError:
            return 401
        except ValueError:
            return 503
        return 200 if hmac.compare_digest(expected, received) else 401


def security_headers(handler, status):
    for name, value in SECURITY_HEADERS.items():
        handler.send_header(name, value)
    if status == 401:
        handler.send_header('WWW-Authenticate', 'Bearer realm="banking"')
