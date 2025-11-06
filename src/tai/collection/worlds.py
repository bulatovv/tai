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
                            'INSERT INTO worlds_online (name, players, static, ssmp, saved_at) VALUES (?, ?, ?, ?, ?)',
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
    active_sessions: dict[str, int] = {}
    suspended_sessions: dict[
        str, tuple[int, int]
    ] = {}  # world_name -> (start_ts, suspended_ts)
    prev_worlds: set[str] = set()
    is_first_iteration = True

    async for data in receiver:
        now = int(time.time())
        current_worlds = {_strip_hex_codes(w['name']) for w in data['worlds']}

        # --- Startup/Recovery Logic (First Iteration Only) ---
        if is_first_iteration:
            log.info('world_session_recovery_started')
            recovered_worlds = set()
            if current_worlds:
                with get_connection(db_path) as con:
                    threshold_time = datetime.fromtimestamp(
                        now - session_threshold, tz=timezone.utc
                    ).replace(tzinfo=None)
                    query = """
                        SELECT
                            name,
                            ARG_MAX(session_start, session_end) AS latest_start
                        FROM world_sessions
                        WHERE name IN (SELECT * FROM UNNEST(?))
                        GROUP BY name
                        HAVING MAX(session_end) > ?
                    """
                    recoverable = con.execute(
                        query, (list(current_worlds), threshold_time)
                    ).fetchall()

                    for world_name, session_start in recoverable:
                        active_sessions[world_name] = int(session_start.timestamp())
                        recovered_worlds.add(world_name)
                        log.debug(
                            'world_session_recovered',
                            world=world_name,
                            session_start=session_start.isoformat(),
                        )

            newly_connected = current_worlds - recovered_worlds
            is_first_iteration = False
        else:
            newly_connected = current_worlds - prev_worlds

        disconnected = prev_worlds - current_worlds

        # --- Handle Disconnected Worlds (Session Suspension) ---
        for world in disconnected:
            if world in active_sessions:
                start_time = active_sessions.pop(world)
                suspended_sessions[world] = (start_time, now)
                log.debug(
                    'world_session_suspended',
                    world=world,
                    start_time=datetime.fromtimestamp(start_time).isoformat(),
                )

        # --- Handle Newly Connected Worlds (Session Resume or New) ---
        if newly_connected:
            with get_connection(db_path) as con:
                for world in newly_connected:
                    if world in suspended_sessions:
                        start_time, _ = suspended_sessions.pop(world)
                        active_sessions[world] = start_time
                        log.debug(
                            'world_session_renewed',
                            world=world,
                            start_time=datetime.fromtimestamp(start_time).isoformat(),
                        )
                    else:
                        start_time = now
                        active_sessions[world] = start_time
                        dt_start = datetime.fromtimestamp(start_time, tz=timezone.utc).replace(
                            tzinfo=None
                        )
                        con.execute(
                            'INSERT INTO world_sessions (name, session_start, session_end) VALUES (?, ?, ?)',
                            (world, dt_start, dt_start),
                        )
                        log.debug(
                            'world_session_started',
                            world=world,
                            session_start=dt_start.isoformat(),
                        )

        # --- Finalize Expired Suspended Sessions ---
        for world, (start_time, suspended_at) in list(suspended_sessions.items()):
            if now - suspended_at > session_threshold:
                del suspended_sessions[world]
                log.debug(
                    'world_session_saved',
                    world=world,
                    start_time=datetime.fromtimestamp(start_time).isoformat(),
                )

        # --- Update Active Sessions ---
        if active_sessions:
            with get_connection(db_path) as con:
                dt_now = datetime.fromtimestamp(now, tz=timezone.utc).replace(tzinfo=None)
                update_data = [
                    (
                        dt_now,
                        world,
                        datetime.fromtimestamp(start_ts, tz=timezone.utc).replace(tzinfo=None),
                    )
                    for world, start_ts in active_sessions.items()
                ]
                con.executemany(
                    'UPDATE world_sessions SET session_end = ? WHERE name = ? AND session_start = ?',
                    update_data,
                )
                log.debug('active_world_sessions_updated', count=len(active_sessions))

        prev_worlds = current_worlds


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
