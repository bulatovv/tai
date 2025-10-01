import time
from datetime import datetime, timezone

import samp_query
import trio

from tai.database.connector import get_connection
from tai.logging import log
from tai.settings import settings


def format_timestamp(timestamp: int) -> str:
    """Format Unix timestamp as ISO datetime string.

    Parameters
    ----------
    timestamp : int
        Unix timestamp to format.

    Returns
    -------
    str
        ISO formatted datetime string.
    """
    return datetime.fromtimestamp(timestamp).isoformat()


async def collect_sessions(db_path: str, session_threshold=1800):
    """Continuously monitor player connections and track gaming sessions.

    Parameters
    ----------
    db_path : str
        Path to the database for storing session data.
    session_threshold : int, default 3600
        Time in seconds after disconnect before session is considered ended (1 hour).

    Returns
    -------
    None
        Runs indefinitely until cancelled.
    """
    log.info('sessions_collection_started')
    client = samp_query.Client(ip=settings.training_host, port=settings.training_port)
    players: set[str] = set()
    prev_players: set[str] = set()

    sessions: dict[str, tuple[int, int | None]] = {}
    while True:
        with trio.move_on_after(10):
            try:
                players = set(p.name for p in (await client.players()).players)
            except trio.Cancelled:
                log.debug('samp_query_timeout')
                continue

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
            with get_connection(db_path) as con:
                con.executemany(
                    'insert into sessions (player, session_start, session_end) values (?, ?, ?)',
                    sessions_to_write,
                )

        await trio.sleep(60)
