import zoneinfo
from datetime import datetime
from typing import Any

import httpx
import polars as pl
import trio

from tai.database.connector import get_connection
from tai.logging import log
from tai.settings import settings


def _preproc_timestamp(timestamp: str | int) -> datetime | None:
    if timestamp == '1970-01-01 03:00:00':
        return None
    if timestamp == 0:
        return None
    moscow_tz = zoneinfo.ZoneInfo('Europe/Moscow')
    dt_moscow = (
        datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        if isinstance(timestamp, str)
        else datetime.fromtimestamp(timestamp)
    ).replace(tzinfo=moscow_tz)
    dt_utc = dt_moscow.astimezone(zoneinfo.ZoneInfo('UTC'))

    # Remove timezone awareness
    dt_utc_naive = dt_utc.replace(tzinfo=None)
    return dt_utc_naive


def _preproc_player(player: dict[str, Any]) -> dict[str, Any]:
    preprocessed_record = player | {
        'lastlogin': _preproc_timestamp(player['lastlogin']),
        'playerid': player['playerid'] if player['online'] else None,
        'regdate': _preproc_timestamp(player['regdate']),
        'warn': [
            warn | {'bantime': _preproc_timestamp(warn['bantime'])} for warn in player['warn']
        ],
        'verify_text': player['verifyText'],
    }
    for key in ('access', 'online', 'playerid', 'verifyText'):
        preprocessed_record.pop(key)

    return preprocessed_record


async def _fetch_first_page(
    client: httpx.AsyncClient,
) -> tuple[list[dict[str, Any]], int]:
    base_url = settings.training_api_base_url
    r = await client.get(f'{base_url}/user')
    first = r.json()
    pages = first['meta']['last_page']
    return list(map(_preproc_player, r.json()['data'])), pages


async def _fetch_players_page(client: httpx.AsyncClient, page: int) -> list[dict[str, Any]]:
    base_url = settings.training_api_base_url
    max_retry_attempts = 5
    retry_delay = 1
    r = None
    for retry_attempt in range(1, max_retry_attempts + 1):
        try:
            r = await client.get(f'{base_url}/user?page={page}')
            r.raise_for_status()
            break
        except Exception as e:
            if r and isinstance(e, httpx.HTTPStatusError) and r.status_code == 429:
                delay = int(r.headers['Retry-After'])
            else:
                delay = retry_delay

            log.warning(
                'fetch_players_page_failed',
                retry=retry_attempt,
                of=max_retry_attempts,
                waiting_for=delay,
                error=type(e).__name__,
                message=e,
            )
            await trio.sleep(delay)
    else:
        raise TimeoutError('All retry attempts failed')

    return list(map(_preproc_player, r.json()['data']))


async def collect_players(db_path: str, temp_db_path: str):
    """
    Collect all player data from the training server API and insert into the database.

    This function fetches player data into a temporary database file first, then uses
    the ATTACH command to transfer the data to the main database. This minimizes
    the time the main database file is locked.
    """
    log.info('players_collection_started')
    snapshot_time = datetime.now()

    with get_connection(temp_db_path) as temp_con:
        temp_con.execute('DELETE FROM players')
        async with httpx.AsyncClient() as client:
            first, total_pages = await _fetch_first_page(client)
            log.debug('fetch_players_page', page=1, of=total_pages)

            for row in first:
                row['snapshot_time'] = snapshot_time

            _ = pl.from_dicts(first)
            temp_con.execute('INSERT INTO players BY NAME SELECT * FROM _')
            await trio.sleep(0.6)

            for page in range(2, total_pages + 1):
                log.debug('fetch_players_page', page=page, of=total_pages)
                page_data = await _fetch_players_page(client, page)
                for row in page_data:
                    row['snapshot_time'] = snapshot_time

                _ = pl.from_dicts(page_data)
                temp_con.execute('INSERT INTO players BY NAME SELECT * FROM _')
                await trio.sleep(0.6)

    with get_connection(db_path) as main_con:
        main_con.execute(f"ATTACH '{temp_db_path}' AS temp_db")
        main_con.execute('INSERT INTO players SELECT * FROM temp_db.players')
        inserted_count = main_con.execute('SELECT COUNT(*) FROM temp_db.players').fetchone()[0]
        main_con.execute('DETACH temp_db')

    log.info('players_collection_completed', inserted=inserted_count)
