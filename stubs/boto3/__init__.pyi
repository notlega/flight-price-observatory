from typing import Any

def client(
    service: str,
    *,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> Any: ...
