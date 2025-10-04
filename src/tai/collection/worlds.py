import re
import time
from datetime import datetime, timezone
from typing import NoReturn

import httpx
import trio

from tai.database.connector import get_connection
from tai.logging import log
from tai.settings import settings


def strip_hex_codes(text: str) -> str:
    """Remove hex color codes from a string.

    Parameters
    ----------
    text
        The string to remove hex codes from.

    Returns
    -------
    str
        The string with hex codes removed.
    """
    return re.sub(r'\{([0-9a-fA-F]{6})\}', '', text)


async def collect_worlds_online(db_path: str, delay: int = 60) -> NoReturn:
    """
    Continuously monitor world data and save changes.

    Parameters
    ----------
    db_path
        Path to the database for storing world data.
    delay
        Time in seconds to wait between API queries.
    """
    log.info('worlds_online_collection_started')
    previous_worlds = {}
    url = f'{settings.chrono_api_base_url}/worlds'

    async with httpx.AsyncClient() as client:
        while True:
            try:
                response = await client.get(
                    url,
                    headers={
                        'X-Login': settings.chrono_login,
                        'X-Token': settings.chrono_token,
                    },
                )
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPError as e:
                log.exception('failed_to_fetch_worlds_data', error=e)
                await trio.sleep(delay)
                continue

            current_worlds = {}
            for world in data['worlds']:
                world_name = strip_hex_codes(world['name'])
                world_data = {
                    'players': world['players'],
                    'static': world['static'],
                    'ssmp': world['ssmp'],
                }
                if (
                    world_name not in current_worlds
                    or world_data['players'] > current_worlds[world_name]['players']
                ):
                    current_worlds[world_name] = world_data

            if current_worlds != previous_worlds:
                with get_connection(db_path) as con:
                    saved_count = 0
                    for world_name, world_data in current_worlds.items():
                        if previous_worlds.get(world_name) != world_data:
                            saved_at = datetime.now(timezone.utc).replace(
                                microsecond=0, tzinfo=None
                            )
                            con.execute(
                                'INSERT OR REPLACE INTO worlds_online (name, players, static, ssmp, saved_at) VALUES (?, ?, ?, ?, ?)',
                                (
                                    world_name,
                                    world_data['players'],
                                    world_data['static'],
                                    world_data['ssmp'],
                                    saved_at,
                                ),
                            )
                            saved_count += 1
                    if saved_count > 0:
                        log.info('worlds_data_saved', count=saved_count)

                previous_worlds = current_worlds

            await trio.sleep(delay)


async def collect_world_sessions(
    db_path: str, session_threshold: int = 1800, delay: int = 60
) -> NoReturn:
    """
    Continuously monitor world connections and track gaming sessions.

    Parameters
    ----------
    db_path
        Path to the database for storing session data.
    session_threshold
        Time in seconds after disconnect before session is considered ended (30 minutes).
    delay
        Time in seconds to wait between server queries.
    """
    log.info('world_sessions_collection_started')
    url = f'{settings.chrono_api_base_url}/worlds'
    worlds: set[str] = set()
    prev_worlds: set[str] = set()

    sessions: dict[str, tuple[int, int | None]] = {}
    async with httpx.AsyncClient() as client:
        while True:
            try:
                response = await client.get(
                    url,
                    headers={
                        'X-Login': settings.chrono_login,
                        'X-Token': settings.chrono_token,
                    },
                )
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPError as e:
                log.exception('failed_to_fetch_worlds_data', error=e)
                await trio.sleep(delay)
                continue

            worlds = {strip_hex_codes(w['name']) for w in data['worlds']}

            newly_connected = worlds - prev_worlds
            disconnected = prev_worlds - worlds
            prev_worlds = worlds

            for world in newly_connected:
                session_start = int(time.time())
                if world not in sessions:
                    log.debug(
                        'world_session_started',
                        world=world,
                        session_start=datetime.fromtimestamp(session_start).isoformat(),
                    )
                else:
                    log.debug(
                        'world_session_renewed',
                        world=world,
                        session_start=datetime.fromtimestamp(session_start).isoformat(),
                    )

                sessions[world] = (session_start, None)

            for world in disconnected:
                session_start, session_end = sessions.pop(world)
                assert session_end is None
                session_end = int(time.time())
                sessions[world] = session_start, session_end
                log.debug(
                    'world_session_suspended',
                    world=world,
                    session_start=datetime.fromtimestamp(session_start).isoformat(),
                    session_end=datetime.fromtimestamp(session_end).isoformat(),
                )

            sessions_to_write = []
            for world in list(sessions):
                session_start, session_end = sessions[world]
                if session_end is None:
                    continue

                if time.time() - session_end > session_threshold:
                    sessions.pop(world)
                    log.debug(
                        'world_session_saved',
                        world=world,
                        session_start=datetime.fromtimestamp(session_start).isoformat(),
                        sesssion_end=datetime.fromtimestamp(session_end).isoformat(),
                    )

                    dt_start = datetime.fromtimestamp(session_start, tz=timezone.utc).replace(
                        tzinfo=None
                    )
                    dt_end = datetime.fromtimestamp(session_end, tz=timezone.utc).replace(
                        tzinfo=None
                    )
                    sessions_to_write.append((world, dt_start, dt_end))

            if sessions_to_write:
                with get_connection(db_path) as con:
                    con.executemany(
                        'insert into world_sessions (name, session_start, session_end) values (?, ?, ?)',
                        sessions_to_write,
                    )

            await trio.sleep(delay)
