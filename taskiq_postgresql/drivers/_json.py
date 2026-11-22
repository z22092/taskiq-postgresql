# pydantic_core.to_json is public API (in pydantic_core.__all__) and is the
# serializer behind pydantic's own JSON output. Each pydantic release pins
# the exact pydantic-core it ships with, so the declared pydantic range also
# covers this import.
from pydantic_core import to_json


def dumps(value: dict) -> str:
    """Encode a JSONB parameter as pydantic would."""
    return to_json(value).decode()
