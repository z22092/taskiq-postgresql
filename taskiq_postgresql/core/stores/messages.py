"""The message store: the broker's table, reached through its query driver."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from taskiq_postgresql.core.sql.catalog import (
    ClaimByIdQuery,
    ClaimColumns,
    ClaimDueQuery,
    notify_statement,
)

if TYPE_CHECKING:
    from taskiq import BrokerMessage

    from taskiq_postgresql.abc.driver import QueryDriver
    from taskiq_postgresql.broker import Table
    from taskiq_postgresql.core.sql.identifier import Identifier
    from taskiq_postgresql.core.stores.schema import SchemaStore


class MessageStore:
    """
    The messages of one broker.

    Attributes:
        schema: The store of the table, which starts, closes and describes it.
        claim_due_statement: The claim of the oldest due message of a channel.
        claim_by_id_statement: The claim of the message a NOTIFY named.
    """

    def __init__(self, schema: SchemaStore, table: Table) -> None:
        """
        Build the claim statements of the table.

        Args:
            schema: The store of the table.
            table: The columns of the broker's table.
        """
        self.schema = schema
        self._table = table
        identifier = schema.declaration.table
        claim_columns = ClaimColumns(
            channel=table.channel,
            not_before=table.not_before,
        )
        returning = [table.message]
        self.claim_due_statement = ClaimDueQuery(identifier).statement(
            table.primary_key,
            claim_columns,
            returning,
        )
        self.claim_by_id_statement = ClaimByIdQuery(identifier).statement(
            table.primary_key,
            claim_columns,
            returning,
        )

    @property
    def driver(self) -> QueryDriver:
        """The query driver in use."""
        return self.schema.driver

    def bind(self, dsn: str) -> None:
        """Point the driver at ``dsn`` and tell it the broker owns the table."""
        self.schema.bind(dsn)

    def configure(self, query_timeout: float) -> None:
        """
        Hand the driver the kind of the table and the broker's query timeout.

        Args:
            query_timeout: Seconds a claim and a close may wait.
        """
        # Attributes, not constructor arguments: the bundled drivers pass every
        # keyword argument on to the connection.
        self.driver.component_kind = self.schema.kind
        self.driver.query_timeout = query_timeout

    async def start(self) -> None:
        """Check or migrate the table."""
        await self.schema.start()

    async def close(self) -> None:
        """Close the driver's pool."""
        await self.schema.close()

    async def insert(
        self,
        message: BrokerMessage,
        channel: str,
        delay_seconds: float,
    ) -> Any:
        """
        Store a message for ``channel``, due ``delay_seconds`` from now.

        Args:
            message: The message to store.
            channel: The channel name, as configured.
            delay_seconds: Seconds from now until the message is due.

        Returns:
            The id of the new row.
        """
        columns = self._table
        return await self.driver.insert(
            [
                columns.task_id,
                columns.task_name,
                columns.message,
                columns.labels,
                columns.channel,
                columns.not_before,
            ],
            [
                message.task_id,
                message.task_name,
                message.message,
                message.labels,
                channel,
                delay_seconds,
            ],
            [columns.primary_key],
        )

    async def notify(self, channel: Identifier, message_id: int) -> None:
        """
        Announce a message on ``channel`` with a NOTIFY.

        Args:
            channel: The channel to notify.
            message_id: The id of the stored message, sent as the payload.
        """
        await self.driver.execute(
            notify_statement().text,
            channel.name,
            str(message_id),
        )

    async def claim_due(self, channel: str) -> Optional[dict[str, Any]]:
        """
        Delete and return the oldest due message of ``channel``.

        Args:
            channel: The channel name, as configured.

        Returns:
            The row, or ``None`` when no message is due.
        """
        return await self.driver.fetch_row(self.claim_due_statement.text, channel)

    async def claim_by_id(
        self,
        channel: str,
        message_id: int,
    ) -> Optional[dict[str, Any]]:
        """
        Delete and return the message ``message_id`` if it is due.

        Args:
            channel: The channel name, as configured.
            message_id: The id a NOTIFY named.

        Returns:
            The row, or ``None`` when that message is not due or is gone.
        """
        return await self.driver.fetch_row(
            self.claim_by_id_statement.text,
            channel,
            message_id,
        )
