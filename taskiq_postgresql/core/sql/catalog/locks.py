"""The migration lock, the lock timeout lines and the transaction around them."""

from typing import Final, NamedTuple, Sequence

from taskiq_postgresql.core.sql.catalog.base import QueryBase, _sql_literal
from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.core.sql.statement import ParamType, Script, Statement

# The server keeps lock_timeout as an int of milliseconds and refuses a larger
# value when the script runs.
MAX_LOCK_TIMEOUT: Final = 2_147_483
# A custom setting (two-part name) that holds a transaction's own lock_timeout
# while steps that share the transaction run with theirs.
SAVED_LOCK_TIMEOUT: Final = "taskiq_postgresql_step.lock_timeout"

# The lock key is a contract between releases: every release takes
# pg_advisory_xact_lock(hashtext('taskiq_postgresql'), hashtext(<table name as
# configured>)), so two releases running side by side, as in a rolling deploy,
# still serialize their migrations. Changing the SQL or the namespace breaks it.
MIGRATION_LOCK_NAMESPACE: Final = "taskiq_postgresql"

# One template for both forms: the query binds the two arguments, the script
# writes them as literals.
_LOCK_TEMPLATE: Final = "SELECT pg_advisory_xact_lock(hashtext({ns}), hashtext({key}))"


def check_lock_timeout(seconds: int) -> int:
    """
    Refuse a lock timeout that no migration could set.

    Args:
        seconds: The seconds a statement waits for a lock; ``0`` for none.

    Returns:
        ``seconds``, unchanged.

    Raises:
        ValueError: if ``seconds`` is not a whole number from 0 to 2147483
            (the server keeps it as milliseconds in an int).
    """
    # A bool is an int, and True would read as one second.
    if (
        isinstance(seconds, bool)
        or not isinstance(seconds, int)
        or not 0 <= seconds <= MAX_LOCK_TIMEOUT
    ):
        raise ValueError(
            "lock_timeout must be a whole number of seconds from 0 to "
            f"{MAX_LOCK_TIMEOUT}, got {seconds!r}",
        )
    return seconds


def migration_lock_script(table: Identifier) -> Script:
    """
    Return the lock ``MigrationLockQuery`` takes, with its arguments as literals.

    A script has no bind parameters.

    Args:
        table: The checked table name; the key is the name as configured.

    Returns:
        The script.
    """
    return Script(
        _LOCK_TEMPLATE.format(
            ns=_sql_literal(MIGRATION_LOCK_NAMESPACE),
            key=_sql_literal(table.configured),
        ),
    )


class MigrationLockQuery(QueryBase):
    """Query taking the transaction lock that serializes migrations of a table."""

    def make_query(self) -> str:
        """Return the query as a string."""
        return _LOCK_TEMPLATE.format(ns="$1", key="$2")

    def statement(self) -> Statement:
        """Return the query typed; its result is ``void``, so it has no column."""
        return Statement(self.make_query(), (ParamType.TEXT, ParamType.TEXT))

    def params(self) -> tuple[str, str]:
        """Return the lock key arguments, in the order of the query."""
        return (MIGRATION_LOCK_NAMESPACE, self.table_name)


def in_transaction(statements: Sequence[str]) -> list[str]:
    """Return ``statements`` between ``BEGIN`` and ``COMMIT``."""
    return ["BEGIN", *statements, "COMMIT"]


class LockTimeoutScripts(NamedTuple):
    """The scripts that bound the lock waits of the steps of one transaction."""

    set_local: Script
    save: Script
    restore: Script


def lock_timeout_scripts(lock_timeout: int) -> LockTimeoutScripts:
    """
    Return the scripts that bound lock waits to ``lock_timeout`` seconds.

    Args:
        lock_timeout: Seconds a statement waits for a lock.

    Returns:
        ``set_local``, which bounds the waits that follow it in the
        transaction; ``save``, which keeps the transaction's own
        ``lock_timeout``; and ``restore``, which gives the kept value back.

    Raises:
        ValueError: if ``lock_timeout`` is not a whole number of seconds from
            0 to 2147483.
    """
    check_lock_timeout(lock_timeout)
    saved = _sql_literal(SAVED_LOCK_TIMEOUT)
    # missing_ok and NULLIF turn a value this transaction never saved (missing
    # or empty) into NULL, which set_config reads as a reset, instead of an
    # error.
    return LockTimeoutScripts(
        set_local=Script(f"SET LOCAL lock_timeout = '{lock_timeout}s'"),
        save=Script(
            f"SELECT set_config({saved}, current_setting('lock_timeout'), true)",
        ),
        restore=Script(
            "SELECT set_config('lock_timeout', "
            f"NULLIF(current_setting({saved}, true), ''), true)",
        ),
    )
