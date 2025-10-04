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

    Parameters
    ----------
    sessions_db_path
        Path to the database for storing session data.
    online_db_path
        Path to the database for storing online count data.
    session_threshold
        Time in seconds after disconnect before session is considered ended (45 minutes).
    delay
        Time in seconds to wait between server queries.
    """
    log.info('sessions_collection_started')
    players: set[str] = set()
    prev_players: set[str] = set()
    previous_online_count = None

    sessions: dict[str, tuple[int, int | None]] = {}
    while True:
        with trio.move_on_after(10):
            try:
                client = create_client()
                players = set(p.name for p in (await client.players()).players)
            except trio.Cancelled:
                log.debug('samp_query_timeout')
                continue
            except Exception:
                log.exception('failed_to_query_server_players')
                continue

        current_online_count = len(players)
        if current_online_count != previous_online_count:
            queried_at = datetime.now(timezone.utc).replace(microsecond=0).replace(tzinfo=None)
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
        prev_players = players

        for player in newly_connected:
            session_start = int(time.time())
            if player not in sessions:
                log.debug(
                    'session_started',
                    player=player,
                    session_start=format_timestamp(session_start),
                )
            else:
                log.debug(
                    'session_renewed',
                    player=player,
                    session_start=format_timestamp(session_start),
                )

            sessions[player] = (session_start, None)

        for player in disconnected:
            session_start, session_end = sessions.pop(player)
            assert session_end is None
            session_end = int(time.time())
            sessions[player] = session_start, session_end
            log.debug(
                'session_suspended',
                player=player,
                session_start=format_timestamp(session_start),
                session_end=format_timestamp(session_end),
            )

        sessions_to_write = []
        for player in list(sessions):
            session_start, session_end = sessions[player]
            if session_end is None:
                continue

            if time.time() - session_end > session_threshold:
                sessions.pop(player)
                log.debug(
                    'session_saved',
                    player=player,
                    session_start=format_timestamp(session_start),
                    sesssion_end=format_timestamp(session_end),
                )

                dt_start = datetime.fromtimestamp(session_start, tz=timezone.utc).replace(
                    tzinfo=None
                )
                dt_end = datetime.fromtimestamp(session_end, tz=timezone.utc).replace(
                    tzinfo=None
                )
                sessions_to_write.append((player, dt_start, dt_end))

        if sessions_to_write:
            with get_connection(sessions_db_path) as con:
                con.executemany(
                    'insert into sessions (player, session_start, session_end) values (?, ?, ?)',
                    sessions_to_write,
                )

        await trio.sleep(delay)
