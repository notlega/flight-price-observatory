"""Export stored search results to JSONL and optional raw payloads."""

import json
import logging
from datetime import datetime
from typing import Any

from collector.repository import SearchRepository

logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR = "storage/raw"

_BUFFER_FLUSH = 1000


def default_output_path() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{DEFAULT_RAW_DIR}/search_{ts}.jsonl"


def _jsonl_row(row: dict[str, Any]) -> str:
    head = json.dumps(
        {
            k: row[k]
            for k in (
                "route",
                "dep_date",
                "return_date",
                "flight_type",
                "origin",
                "destination",
            )
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    tail = json.dumps({"searched_at": row["searched_at"]}, separators=(",", ":"))
    # flights is already serialised JSON; splice it in unescaped.
    return f'{head[:-1]},"flights":{row["flights"]},{tail[1:]}\n'


async def convert(
    db_path: str, output_path: str | None = None, delete: bool = False
) -> str:
    if output_path is None:
        output_path = default_output_path()

    repo = SearchRepository(db_path)
    await repo.open()

    total = await repo.count_successful()
    if total == 0:
        failed = await repo.count_failed()
        if failed:
            logger.warning(
                "No successful results to convert (%d failed tasks, none retryable)",
                failed,
            )
        else:
            logger.warning("No successful results to convert")
        if delete:
            await repo.delete_db()
        else:
            await repo.close()
        return output_path

    written = 0
    buffer: list[str] = []
    try:
        with open(output_path, "w") as f:
            async for row in repo.iter_successful_raw():
                buffer.append(_jsonl_row(row))
                written += 1
                if len(buffer) >= _BUFFER_FLUSH:
                    f.writelines(buffer)
                    buffer.clear()
            if buffer:
                f.writelines(buffer)
    except Exception:
        logger.exception("Failed writing %s; state database retained", output_path)
        await repo.close()
        raise

    logger.info("Wrote %d rows to %s", written, output_path)

    if delete:
        await repo.delete_db()
    else:
        await repo.close()

    return output_path
