# TaskIQ PostgreSQL

TaskIQ PostgreSQL is a comprehensive plugin for [TaskIQ](https://taskiq-python.github.io/) that provides PostgreSQL-based broker, result backend, and scheduler source with support for multiple PostgreSQL drivers.

## Features

- **🚀 PostgreSQL Broker**: High-performance message broker using PostgreSQL LISTEN/NOTIFY
- **📦 Result Backend**: Persistent task result storage with configurable retention
- **⏰ Scheduler Source**: Cron-like task scheduling with PostgreSQL persistence
- **🔌 Multiple Drivers**: Support for asyncpg, psycopg3, and psqlpy
- **⚡ Async/Await**: Built for high-performance async operations
- **🛠️ Flexible Configuration**: Customizable table names, field types, and connection options
- **🔄 Multiple Serializers**: Support for different serialization methods (Pickle, JSON, etc.)
- **🔐 Connection Pooling**: Built-in connection pool management for all drivers

## Installation

### Basic Installation

```bash
pip install taskiq-postgresql
```

### With Driver Dependencies

Choose your preferred PostgreSQL driver:

**AsyncPG (Recommended)**
```bash
pip install taskiq-postgresql[asyncpg]
```

**Psycopg3**
```bash
pip install taskiq-postgresql[psycopg]
```

**PSQLPy**
```bash
pip install taskiq-postgresql[psqlpy]
```



### Using Package Managers

**Poetry:**
```bash
poetry add taskiq-postgresql[asyncpg]
```

**UV:**
```bash
uv add taskiq-postgresql[asyncpg]
```

**Rye:**
```bash
rye add taskiq-postgresql[asyncpg]
```

> **Note**: Driver extras are required as PostgreSQL drivers are optional dependencies. Without them, the PostgreSQL drivers won't be available.

## Quick Start

### Basic Task Processing

```python
import asyncio
from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend

# Configure the result backend
result_backend = PostgresqlResultBackend(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db",
)

# Configure the broker with result backend
broker = PostgresqlBroker(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db",
).with_result_backend(result_backend)


@broker.task
async def calculate_sum(a: int, b: int) -> int:
    """Calculate the sum of two numbers."""
    await asyncio.sleep(1)  # Simulate some work
    return a + b


async def main():
    # Startup the broker
    await broker.startup()
    
    # Send a task
    task = await calculate_sum.kiq(10, 20)
    
    # Wait for result
    result = await task.wait_result()
    print(f"Result: {result}")  # Result: 30
    
    # Shutdown the broker
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Task Scheduling

```python
from taskiq_postgresql import PostgresqlBroker, PostgresqlSchedulerSource
from taskiq import TaskiqScheduler

# Initialize broker
broker = PostgresqlBroker(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db"
)

# Initialize scheduler source
scheduler_source = PostgresqlSchedulerSource(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db",
    table_name="taskiq_schedules",
    driver="asyncpg"
)

# Create scheduler
scheduler = TaskiqScheduler(
    broker=broker,
    sources=[scheduler_source],
)

@broker.task
async def scheduled_task():
    print("This task runs on schedule!")

# Schedule task to run every minute
async def setup_schedule():
    await scheduler_source.add_schedule(
        schedule_id="task-every-minute",
        task_name="scheduled_task",
        cron="* * * * *",  # Every minute
        args=[],
        kwargs={}
    )
```

## Configuration

### PostgresqlBroker

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dsn` | `str` | Required | PostgreSQL connection string |
| `queue_name` | `str` | `"taskiq_queue"` | Name of the queue table |
| `field_for_task_id` | `Literal["VarChar", "Text", "Uuid"]` | `"Uuid"` | Field type for task IDs |
| `driver` | `Literal["asyncpg", "psycopg", "psqlpy"]` | `"asyncpg"` | Database driver |
| `**connect_kwargs` | `Any` | - | Additional driver-specific connection parameters |

### PostgresqlResultBackend

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dsn` | `str` | Required | PostgreSQL connection string |
| `keep_results` | `bool` | `True` | Whether to keep results after reading |
| `table_name` | `str` | `"taskiq_results"` | Name of the results table |
| `field_for_task_id` | `Literal["VarChar", "Text", "Uuid"]` | `"Uuid"` | Field type for task IDs |
| `serializer` | `BaseSerializer` | `PickleSerializer()` | Serializer instance |
| `driver` | `Literal["asyncpg", "psycopg", "psqlpy"]` | `"asyncpg"` | Database driver |
| `**connect_kwargs` | `Any` | - | Additional driver-specific connection parameters |

### PostgresqlSchedulerSource

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dsn` | `str` | Required | PostgreSQL connection string |
| `table_name` | `str` | `"taskiq_schedules"` | Name of the schedules table |
| `driver` | `Literal["asyncpg", "psycopg", "psqlpy"]` | `"asyncpg"` | Database driver |
| `startup_schedule` | `dict` | `None` | Schedule definitions to create on startup |
| `**connect_kwargs` | `Any` | - | Additional driver-specific connection parameters |

## Database Drivers

### AsyncPG (Recommended)
- **Performance**: Fastest PostgreSQL driver for Python
- **Features**: Full asyncio support, prepared statements, connection pooling
- **Use case**: High-performance applications

### Psycopg3
- **Performance**: Good performance with extensive features
- **Features**: Full PostgreSQL feature support, mature ecosystem
- **Use case**: Feature-rich applications needing advanced PostgreSQL features

### PSQLPy
- **Performance**: Rust-based driver with excellent performance
- **Features**: Modern async implementation
- **Use case**: Applications prioritizing performance and modern architecture



## Advanced Configuration

### Custom Serializer

```python
from taskiq.serializers import JSONSerializer

result_backend = PostgresqlResultBackend(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db",
    serializer=JSONSerializer(),
)
```

### Custom Table Names and Field Types

```python
broker = PostgresqlBroker(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db",
    queue_name="my_custom_queue",
    field_for_task_id="Text",  # Use TEXT instead of UUID
)

result_backend = PostgresqlResultBackend(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db",
    table_name="my_custom_results",
    field_for_task_id="VarChar",  # Use VARCHAR instead of UUID
)
```

### Connection Pool Configuration

```python
# AsyncPG
broker = PostgresqlBroker(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db",
    driver="asyncpg",
    min_size=5,
    max_size=20,
    max_inactive_connection_lifetime=300,
)

# Psycopg3
broker = PostgresqlBroker(
    dsn="postgresql://postgres:postgres@localhost:5432/taskiq_db",
    driver="psycopg",
    min_size=5,
    max_size=20,
    max_lifetime=3600,
)
```

### Using Environment Variables

```python
import os

# From environment
dsn = os.getenv("DATABASE_URL", "postgresql://localhost/taskiq")

# With SSL
dsn = "postgresql://user:pass@localhost:5432/db?sslmode=require"

broker = PostgresqlBroker(dsn=dsn)
```

## Database Schema

The library automatically creates the necessary tables:

### Queue Table (default: `taskiq_queue`)
```sql
CREATE TABLE taskiq_queue (
    id SERIAL PRIMARY KEY,
    task_id UUID NOT NULL,
    task_name VARCHAR NOT NULL,
    message BYTEA NOT NULL,
    labels JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Results Table (default: `taskiq_results`)
```sql
CREATE TABLE taskiq_results (
    task_id UUID PRIMARY KEY,
    result BYTEA,
    is_err BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Schedules Table (default: `taskiq_schedules`)
```sql
CREATE TABLE taskiq_schedules (
    id UUID PRIMARY KEY,
    task_name VARCHAR(100) NOT NULL,
    schedule JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Performance Tips

1. **Choose the Right Driver**: AsyncPG typically offers the best performance
2. **Connection Pooling**: Configure appropriate pool sizes for your workload
3. **Field Types**: Use UUID for high-performance task IDs, TEXT for debugging
4. **Indexes**: Consider adding indexes on frequently queried columns
5. **Connection Reuse**: Keep broker connections alive during application lifetime

## Troubleshooting

### Common Issues

**Connection Errors**
```python
# Ensure your connection string is correct
dsn = "postgresql://username:password@host:port/database"

# Check PostgreSQL is running and accessible
import asyncpg
conn = await asyncpg.connect(dsn)
await conn.close()
```

**Table Creation Issues**
```python
# Ensure user has CREATE TABLE permissions
# Or manually create tables using provided schemas
```

**Driver Import Errors**
```bash
# Install the appropriate driver extra
pip install taskiq-postgresql[asyncpg]
```

## Requirements

- **Python**: 3.9+
- **TaskIQ**: 0.11.7+
- **PostgreSQL**: 10+

### Driver Dependencies

| Driver | Version | Extra |
|--------|---------|-------|
| AsyncPG | 0.30.0+ | `[asyncpg]` |
| Psycopg3 | 3.2.9+ | `[psycopg]` |
| PSQLPy | 0.11.3+ | `[psqlpy]` |

## Development

This project uses modern Python development tools:

- **[UV](https://github.com/astral-sh/uv)**: Fast Python package installer and resolver
- **[Ruff](https://github.com/astral-sh/ruff)**: Extremely fast Python linter and formatter
- **[Pytest](https://pytest.org/)**: Testing framework

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/z22092/taskiq-postgresql.git
cd taskiq-postgresql

# Install dependencies with UV
uv sync

# Install with all driver extras
uv sync --extra asyncpg --extra psycopg --extra psqlpy

# Run tests
uv run pytest

# Format and lint
uv run ruff format
uv run ruff check
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Links

- **[TaskIQ Documentation](https://taskiq-python.github.io/)** - Main TaskIQ framework
- **[AsyncPG Documentation](https://magicstack.github.io/asyncpg/)** - AsyncPG driver docs
- **[Psycopg Documentation](https://www.psycopg.org/psycopg3/)** - Psycopg3 driver docs  
- **[PostgreSQL Documentation](https://www.postgresql.org/docs/)** - PostgreSQL database docs
- **[GitHub Repository](https://github.com/z22092/taskiq-postgresql)** - Source code and issues
