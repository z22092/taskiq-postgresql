class BaseTaskiqAsyncpgError(Exception):
    """Base error for all possible exception in the lib."""


class DatabaseConnectionError(BaseTaskiqAsyncpgError):
    """Error if cannot connect to PostgreSQL."""


class TransientDatabaseConnectionError(DatabaseConnectionError):
    """Error for a temporary PostgreSQL connection failure."""


class ListenerDisconnectedError(TransientDatabaseConnectionError):
    """Error raised when an established listener connection is lost."""


class ResultIsMissingError(BaseTaskiqAsyncpgError):
    """Error if cannot retrieve result from PostgreSQL."""
