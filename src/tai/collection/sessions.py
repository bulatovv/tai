import time
from datetime import datetime, timezone
from typing import NoReturn

import trio

from tai.database.connector import get_connection
from tai.logging import log
from tai.samp import create_client


def format_timestamp(timestamp: int) -> str:
    """Format Unix timestamp as ISO datetime string.

    Parameters
    ----------
    timestamp
        Unix timestamp to format.

    Returns
    -------
    str
        ISO formatted datetime string.
    """
    return datetime.fromtimestamp(timestamp).isoformat()


async def collect_sessions(
    sessions_db_path: str,
    online_db_path: str,
    session_threshold: int = 2700,
    delay: int = 60,
) -> NoReturn:
    """
    Continuously monitor player connections and track gaming sessions.

    This function persists active sessions to the database by continuously
    updating the `session_end` timestamp. It also handles session suspension,
    allowing players to reconnect within a given threshold to continue their
    session.

    Parameters
    ----------
    sessions_db_path
        Path to the database for storing session data.
    online_db_path
        Path to the database for storing online count data.
    session_threshold
        Time in seconds after disconnect before a session is considered ended.
    delay
        Time in seconds to wait between server queries.
    """
    log.info('sessions_collection_started')
    active_sessions: dict[str, int] = {}
    suspended_sessions: dict[str, tuple[int, int]] = {}  # player -> (start_ts, suspended_ts)

    # --- Startup Recovery Step ---
    try:
        log.info('session_recovery_started')
        client = create_client()
        initial_players = set(p.name for p in (await client.players()).players)

        if initial_players:
            with get_connection(sessions_db_path) as con:
                threshold_time = datetime.fromtimestamp(
                    time.time() - session_threshold, tz=timezone.utc
                ).replace(tzinfo=None)
                query = """
                    SELECT
                        player,
                        ARG_MAX(session_start, session_end) AS latest_start
                    FROM sessions
                    WHERE player IN (SELECT * FROM UNNEST(?))
                    GROUP BY player
                    HAVING MAX(session_end) > ?
                """
                recoverable = con.execute(
                    query, (list(initial_players), threshold_time)
                ).fetchall()

                for player, session_start in recoverable:
                    active_sessions[player] = int(session_start.timestamp())
                    log.debug(
                        'session_recovered',
                        player=player,
                        session_start=session_start.isoformat(),
                    )
    except Exception:
        log.exception('session_recovery_failed')
        initial_players = set()

    prev_players = initial_players
    previous_online_count = None

    # --- Main Collection Loop ---
    while True:
        now = int(time.time())
        try:
            with trio.move_on_after(10):
                client = create_client()
                players = set(p.name for p in (await client.players()).players)
        except trio.Cancelled:
            log.debug('samp_query_timeout')
            await trio.sleep(delay)
            continue
        except Exception:
            log.exception('failed_to_query_server_players')
            await trio.sleep(delay)
            continue

        # --- Online Count Logic ---
        current_online_count = len(players)
        if current_online_count != previous_online_count:
            queried_at = datetime.fromtimestamp(now, tz=timezone.utc).replace(tzinfo=None)
            with get_connection(online_db_path) as con:
                con.execute(
                    'INSERT INTO online (online_count, queried_at) VALUES (?, ?)',
                    (current_online_count, queried_at),
                )
            log.debug(
                'online_count_changed',
                online_count=current_online_count,
                queried_at=queried_at.isoformat(),
            )
            previous_online_count = current_online_count

        newly_connected = players - prev_players
        disconnected = prev_players - players

        # --- Handle Disconnected Players (Session Suspension) ---
        for player in disconnected:
            if player in active_sessions:
                start_time = active_sessions.pop(player)
                suspended_sessions[player] = (start_time, now)
                log.debug(
                    'session_suspended', player=player, start_time=format_timestamp(start_time)
                )

        # --- Handle Newly Connected Players (Session Resume or New) ---
        if newly_connected:
            with get_connection(sessions_db_path) as con:
                for player in newly_connected:
                    if player in suspended_sessions:
                        start_time, _ = suspended_sessions.pop(player)
                        active_sessions[player] = start_time
                        log.debug(
                            'session_renewed',
                            player=player,
                            start_time=format_timestamp(start_time),
                        )
                    else:
                        start_time = now
                        active_sessions[player] = start_time
                        dt_start = datetime.fromtimestamp(start_time, tz=timezone.utc).replace(
                            tzinfo=None
                        )
                        con.execute(
                            'INSERT INTO sessions (player, session_start, session_end) VALUES (?, ?, ?)',
                            (player, dt_start, dt_start),
                        )
                        log.debug(
                            'session_started', player=player, session_start=dt_start.isoformat()
                        )

        # --- Finalize Expired Suspended Sessions ---
        for player, (start_time, suspended_at) in list(suspended_sessions.items()):
            if now - suspended_at > session_threshold:
                del suspended_sessions[player]
                log.debug(
                    'session_ended', player=player, start_time=format_timestamp(start_time)
                )

        # --- Update Active Sessions ---
        if active_sessions:
            with get_connection(sessions_db_path) as con:
                dt_now = datetime.fromtimestamp(now, tz=timezone.utc).replace(tzinfo=None)
                update_data = [
                    (
                        dt_now,
                        player,
                        datetime.fromtimestamp(start_ts, tz=timezone.utc).replace(tzinfo=None),
                    )
                    for player, start_ts in active_sessions.items()
                ]
                con.executemany(
                    'UPDATE sessions SET session_end = ? WHERE player = ? AND session_start = ?',
                    update_data,
                )
                log.debug('active_sessions_updated', count=len(active_sessions))

        prev_players = players
        await trio.sleep(delay)
