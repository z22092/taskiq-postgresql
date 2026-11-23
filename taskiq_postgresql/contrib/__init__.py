"""
Integrations with other libraries, each installed with an extra of its own.

``taskiq_postgresql.contrib.sqlalchemy`` (extra ``sqlalchemy``),
``taskiq_postgresql.contrib.sqlmodel`` (extra ``sqlmodel``) and
``taskiq_postgresql.contrib.alembic`` (extra ``alembic``) build on the table
description of ``taskiq_postgresql.migrations``: they map its structure to
their library and never parse its SQL. The package itself imports none of
them, and each raises ``ImportError`` naming its extra when the library it
needs is missing.
"""
