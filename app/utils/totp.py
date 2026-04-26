"""Helpers for TOTP-based two-factor authentication without extra dependencies."""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
import time
import urllib.parse


def generate_totp_secret(length: int = 20) -> str:
    """Generate a Base32 secret compatible with authenticator apps."""
    secret_bytes = secrets.token_bytes(length)
    return base64.b32encode(secret_bytes).decode('ascii').rstrip('=')


def _normalize_secret(secret: str) -> bytes:
    normalized = ''.join((secret or '').strip().upper().split())
    padding = '=' * ((8 - len(normalized) % 8) % 8)
    return base64.b32decode(normalized + padding, casefold=True)


def generate_totp_code(secret: str, for_time: int | None = None, interval: int = 30, digits: int = 6) -> str:
    """Generate a numeric TOTP code for the given secret and time window."""
    if for_time is None:
        for_time = int(time.time())
    counter = int(for_time // interval)
    key = _normalize_secret(secret)
    counter_bytes = struct.pack('>Q', counter)
    digest = hmac.new(key, counter_bytes, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    binary_code = struct.unpack('>I', digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(binary_code % (10 ** digits)).zfill(digits)


def verify_totp_code(secret: str, code: str, interval: int = 30, digits: int = 6, valid_window: int = 1) -> bool:
    """Validate a TOTP code allowing a small clock drift window."""
    normalized_code = ''.join(ch for ch in str(code or '') if ch.isdigit())
    if len(normalized_code) != digits:
        return False
    current_time = int(time.time())
    for window_offset in range(-valid_window, valid_window + 1):
        candidate_time = current_time + (window_offset * interval)
        if generate_totp_code(secret, candidate_time, interval=interval, digits=digits) == normalized_code:
            return True
    return False


def build_totp_uri(secret: str, username: str, issuer: str) -> str:
    """Build an otpauth URI compatible with Google Authenticator and similar apps."""
    label = urllib.parse.quote(f'{issuer}:{username}')
    issuer_param = urllib.parse.quote(issuer)
    secret_param = urllib.parse.quote((secret or '').strip().upper())
    return f'otpauth://totp/{label}?secret={secret_param}&issuer={issuer_param}&algorithm=SHA1&digits=6&period=30'
