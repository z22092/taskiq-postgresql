"""
The claim statements main runs today, against a real PostgreSQL.

These are the claims 0.5.x will ship, frozen at the 0.5.0 tag; 0.4.x deletes
the notified id regardless of ``not_before``. In a mixed deployment a worker
that claims this way keeps taking rows from a table that a newer release also
writes to, with exactly these two statements. They are frozen here as text, not
built from the current builders, so a change to the builders cannot move them:
what these tests pin is what those workers run.
"""

import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
import pytest

pytestmark = pytest.mark.anyio

# The broker's table as 0.5.x creates it, claim columns included.
# Never change: it is the table the frozen statements below run against.
BROKER_TABLE_0_5_SQL = (
    "CREATE TABLE {table} (id SERIAL NOT NULL PRIMARY KEY, "
    "task_id Uuid NOT NULL, task_name VARCHAR NOT NULL, message BYTEA NOT NULL, "
    "labels JSONB NOT NULL, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), "
    "channel VARCHAR, "
    "not_before TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW())"
)

# The statements 0.5.x runs to claim a row, for the default table.
# They must never change: workers that run them stay beside newer releases.
CLAIM_DUE_SQL_0_5 = (
    "DELETE FROM {table} WHERE id = ("
    "SELECT id FROM {table} "
    "WHERE channel = $1 AND not_before <= NOW() "
    "ORDER BY not_before, id LIMIT 1 FOR UPDATE SKIP LOCKED"
    ") RETURNING message"
)
CLAIM_BY_ID_SQL_0_5 = (
    "DELETE FROM {table} "
    "WHERE id = $2 AND not_before <= NOW() "
    "AND (channel = $1 OR channel IS NULL) "
    "RETURNING message"
)

CHANNEL = "pre_lease_claims"

# What a claim binds for a row id: the sweep binds the channel only, the claim
# by id binds the channel and the id.
ArgumentsFor = Callable[[int], tuple[object, ...]]


def due_arguments(_row_id: int) -> tuple[object, ...]:
    """Return the arguments of the sweep claim: the channel."""
    return (CHANNEL,)


def by_id_arguments(row_id: int) -> tuple[object, ...]:
    """Return the arguments of the claim by id: the channel, then the id."""
    return (CHANNEL, row_id)


CLAIMS = [
    pytest.param(CLAIM_DUE_SQL_0_5, due_arguments, id="claim_due"),
    pytest.param(CLAIM_BY_ID_SQL_0_5, by_id_arguments, id="claim_by_id"),
]


@asynccontextmanager
async def broker_table(dsn: str, table: str) -> AsyncIterator[asyncpg.Connection]:
    """Create ``table`` as 0.5.x does; drop it when the block ends."""
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(BROKER_TABLE_0_5_SQL.format(table=table))
        yield connection
    finally:
        try:
            await connection.execute(f"DROP TABLE {table}")
        finally:
            await connection.close()


async def insert_row(
    connection: asyncpg.Connection,
    table: str,
    *,
    channel: Optional[str],
    delay_seconds: float,
) -> int:
    """Insert a row due ``delay_seconds`` from now and return its id."""
    return await connection.fetchval(
        f"INSERT INTO {table} (task_id, task_name, message, labels, channel, "  # noqa: S608
        "not_before) VALUES ($1, 'claimed', $2, '{}', $3, "
        "NOW() + make_interval(secs => $4::double precision)) RETURNING id",
        uuid.uuid4(),
        b"payload",
        channel,
        delay_seconds,
    )


async def claim(
    connection: asyncpg.Connection,
    statement: str,
    arguments_for: ArgumentsFor,
    table: str,
    row_id: int,
) -> list[asyncpg.Record]:
    """Run a frozen claim with the arguments it binds for ``row_id``."""
    return await connection.fetch(
        statement.format(table=table),
        *arguments_for(row_id),
    )


async def remaining_ids(connection: asyncpg.Connection, table: str) -> list[int]:
    """Return the ids still in ``table``."""
    rows = await connection.fetch(f"SELECT id FROM {table}")  # noqa: S608
    return [row["id"] for row in rows]


@pytest.mark.parametrize(("statement", "arguments_for"), CLAIMS)
async def test_when_a_row_is_due_later__then_the_claim_skips_it(
    postgresql_dsn: str,
    postgres_table: str,
    statement: str,
    arguments_for: ArgumentsFor,
) -> None:
    """The statement of a worker that claims this way leaves a later row alone."""
    async with broker_table(postgresql_dsn, postgres_table) as connection:
        later = await insert_row(
            connection,
            postgres_table,
            channel=CHANNEL,
            delay_seconds=3600,
        )

        claimed = await claim(
            connection,
            statement,
            arguments_for,
            postgres_table,
            later,
        )
        left = await remaining_ids(connection, postgres_table)

    assert claimed == []
    assert left == [later]


@pytest.mark.parametrize(("statement", "arguments_for"), CLAIMS)
async def test_when_a_row_is_due__then_the_claim_deletes_it(
    postgresql_dsn: str,
    postgres_table: str,
    statement: str,
    arguments_for: ArgumentsFor,
) -> None:
    """
    The same statement takes a due row.

    It is the positive control of the test above: an empty result there would
    also be what a statement that matches nothing at all returns.
    """
    async with broker_table(postgresql_dsn, postgres_table) as connection:
        due = await insert_row(
            connection,
            postgres_table,
            channel=CHANNEL,
            delay_seconds=0,
        )

        claimed = await claim(
            connection,
            statement,
            arguments_for,
            postgres_table,
            due,
        )
        left = await remaining_ids(connection, postgres_table)

    assert [row["message"] for row in claimed] == [b"payload"]
    assert left == []


async def test_when_a_row_has_no_channel__then_only_the_claim_by_id_takes_it(
    postgresql_dsn: str,
    postgres_table: str,
) -> None:
    """
    A row written before the channel column existed is claimed by its id.

    Only the claim by id reaches such a row: the sweep filters on the channel.
    """
    async with broker_table(postgresql_dsn, postgres_table) as connection:
        row_id = await insert_row(
            connection,
            postgres_table,
            channel=None,
            delay_seconds=0,
        )
        swept = await claim(
            connection,
            CLAIM_DUE_SQL_0_5,
            due_arguments,
            postgres_table,
            row_id,
        )
        claimed = await claim(
            connection,
            CLAIM_BY_ID_SQL_0_5,
            by_id_arguments,
            postgres_table,
            row_id,
        )
        left = await remaining_ids(connection, postgres_table)

    assert swept == []
    assert [row["message"] for row in claimed] == [b"payload"]
    assert left == []
