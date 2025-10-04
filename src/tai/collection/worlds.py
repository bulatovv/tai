import re
import time
from datetime import datetime, timezone
from typing import Any, NoReturn

import httpx
import trio
from trio import MemoryReceiveChannel, MemorySendChannel

from tai.database.connector import get_connection
from tai.logging import log
from tai.settings import settings


def _strip_hex_codes(text: str) -> str:
    """Remove hex color codes from a string."""
    return re.sub(r'\{([0-9a-fA-F]{6})\}', '', text)


async def _fetch_worlds_data(
    online_worlds_sender: MemorySendChannel[dict[str, Any]],
    world_sessions_sender: MemorySendChannel[dict[str, Any]],
    delay: int = 60,
) -> NoReturn:
    """Continuously fetch world data and send it to the appropriate channels."""
    log.info('worlds_data_fetching_started')
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
                    timeout=8,
                )
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPError as e:
                log.exception('failed_to_fetch_worlds_data', error=e)
                await trio.sleep(delay)
                continue

            await online_worlds_sender.send(data)
            await world_sessions_sender.send(data)
            await trio.sleep(delay)


async def _collect_worlds_online(
    db_path: str,
    receiver: MemoryReceiveChannel[dict[str, Any]],
) -> NoReturn:
    """Continuously monitor world data and save changes."""
    log.info('worlds_online_collection_started')
    previous_worlds = {}

    async for data in receiver:
        current_worlds = {}
        for world in data['worlds']:
            world_name = _strip_hex_codes(world['name'])
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
            for world_name, world_data in current_worlds.items():
                prev_players = previous_worlds.get(world_name, {}).get('players')
                curr_players = world_data.get('players')
                if prev_players and prev_players != curr_players:
                    log.debug(
                        'world_online_count_changed',
                        world_name=world_name,
                        new_count=curr_players,
                        old_count=prev_players,
                    )
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
                    log.debug('worlds_data_saved', count=saved_count)

            previous_worlds = current_worlds


async def _collect_world_sessions(
    db_path: str,
    receiver: MemoryReceiveChannel[dict[str, Any]],
    session_threshold: int = 1800,
) -> NoReturn:
    """Continuously monitor world connections and track gaming sessions."""
    log.info('world_sessions_collection_started')
    worlds: set[str] = set()
    prev_worlds: set[str] = set()

    sessions: dict[str, tuple[int, int | None]] = {}
    async for data in receiver:
        worlds = {_strip_hex_codes(w['name']) for w in data['worlds']}

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


async def collect_worlds(
    online_db_path: str,
    sessions_db_path: str,
    delay: int = 60,
    session_threshold: int = 1800,
) -> NoReturn:
    """Continuously monitor world data and save changes."""
    online_worlds_sender, online_worlds_receiver = trio.open_memory_channel[dict[str, Any]](0)
    world_sessions_sender, world_sessions_receiver = trio.open_memory_channel[dict[str, Any]](0)
    async with trio.open_nursery() as n:
        n.start_soon(_fetch_worlds_data, online_worlds_sender, world_sessions_sender, delay)
        n.start_soon(_collect_worlds_online, online_db_path, online_worlds_receiver)
        n.start_soon(
            _collect_world_sessions,
            sessions_db_path,
            world_sessions_receiver,
            session_threshold,
        )
