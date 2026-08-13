"""Export stored search results to JSONL and optional raw payloads."""

import json
import logging
from datetime import datetime

from collector.repository import SearchRepository

logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR = "storage/raw"


def default_output_path() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{DEFAULT_RAW_DIR}/search_{ts}.jsonl"


def _jsonl_row(row: dict) -> str:
    return (
        '{"route":'
        + json.dumps(row["route"], ensure_ascii=False)
        + ',"dep_date":'
        + json.dumps(row["dep_date"])
        + ',"return_date":'
        + json.dumps(row["return_date"])
        + ',"flight_type":'
        + json.dumps(row["flight_type"])
        + ',"origin":'
        + json.dumps(row["origin"])
        + ',"destination":'
        + json.dumps(row["destination"])
        + ',"flights":'
        + row["flights"]
        + ',"searched_at":'
        + json.dumps(row["searched_at"])
        + "}\n"
    )


async def convert(
    db_path: str, output_path: str | None = None, delete: bool = True
) -> str:
    if output_path is None:
        output_path = default_output_path()

    repo = SearchRepository(db_path)
    await repo.open()

    total = await repo.count_successful()
    if total == 0:
        logger.warning("No successful results to convert")
        if delete:
            await repo.delete_db()
        else:
            await repo.close()
        return output_path

    written = 0
    buffer: list[str] = []
    with open(output_path, "w") as f:
        async for row in repo.iter_successful_raw():
            buffer.append(_jsonl_row(row))
            written += 1
            if len(buffer) >= 1000:
                f.writelines(buffer)
                buffer.clear()
        if buffer:
            f.writelines(buffer)

    logger.info("Wrote %d rows to %s", written, output_path)

    if delete:
        await repo.delete_db()
    else:
        await repo.close()

    return output_path
