"""Security helpers shared across authentication and admin flows."""

from werkzeug.security import check_password_hash, generate_password_hash


HASH_PREFIXES = ('pbkdf2:', 'scrypt:')


def is_password_hashed(raw_value):
    """Return True when the stored password already uses a supported hash format."""
    value = str(raw_value or '').strip()
    return any(value.startswith(prefix) for prefix in HASH_PREFIXES)


def hash_password(password):
    """Hash a plaintext password with Werkzeug's portable default algorithm."""
    return generate_password_hash(str(password or ''))


def verify_password(stored_password, candidate_password):
    """Accept hashed credentials and keep temporary compatibility with plaintext rows."""
    stored_value = str(stored_password or '')
    candidate_value = str(candidate_password or '')
    if is_password_hashed(stored_value):
        return check_password_hash(stored_value, candidate_value)
    return stored_value == candidate_value
