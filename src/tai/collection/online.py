from datetime import datetime, timezone
from typing import NoReturn

import trio

from tai.database.connector import get_connection
from tai.logging import log
from tai.samp import create_client


async def collect_online(db_path: str, delay: int = 60) -> NoReturn:
    """
    Continuously monitor server online count and store it in the database.

    Parameters
    ----------
    db_path
        Path to the database for storing online count data.
    delay
        Time in seconds to wait between server queries.
    """
    log.info('online_collection_started')
    previous_online_count = None

    client = create_client()
    while True:
        current_online_count = None
        try:
            info = await client.info()
            current_online_count = info.players
        except Exception as e:
            log.error('failed_to_query_server_info', error=str(e))

        if current_online_count is not None and current_online_count != previous_online_count:
            queried_at = datetime.now(timezone.utc).replace(tzinfo=None)
            with get_connection(db_path) as con:
                con.execute(
                    'INSERT INTO online (online_count, queried_at) VALUES (?, ?)',
                    (current_online_count, queried_at),
                )
            log.info(
                'online_count_changed',
                online_count=current_online_count,
                queried_at=queried_at.isoformat(),
            )
            previous_online_count = current_online_count

        await trio.sleep(delay)
