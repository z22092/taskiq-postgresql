class BaseTaskiqAsyncpgError(Exception):
    """Base error for all possible exception in the lib."""


class DatabaseConnectionError(BaseTaskiqAsyncpgError):
    """Error if cannot connect to PostgreSQL."""


class ResultIsMissingError(BaseTaskiqAsyncpgError):
    """Error if cannot retrieve result from PostgreSQL."""
