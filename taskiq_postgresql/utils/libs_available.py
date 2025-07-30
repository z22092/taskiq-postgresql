from importlib.util import find_spec


def is_asyncpg_available() -> bool:
    """Check if asyncpg is available."""
    return find_spec("asyncpg") is not None


def is_psqlpy_available() -> bool:
    """Check if psqlpy is available."""
    return find_spec("psqlpy") is not None


def is_psycopg_available() -> bool:
    """Check if psycopg is available."""
    return find_spec("psycopg") is not None
