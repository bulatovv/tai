import time

import samp_query
import trio
from duckdb import DuckDBPyConnection

from tai.logging import log
from tai.settings import settings


async def collect_sessions(con: DuckDBPyConnection, session_threshold=3600):
    """Continuously monitor player connections and track gaming sessions.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Database connection for storing session data.
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
        with trio.move_on_after(3):
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
                log.debug('session_started', player=player, session_start=session_start)
            else:
                log.debug('session_renewed', player=player, session_start=session_start)

            sessions[player] = (session_start, None)

        for player in disconnected:
            session_start, session_end = sessions.pop(player)
            assert session_end is None
            session_end = int(time.time())
            sessions[player] = session_start, session_end
            log.debug(
                'session_suspended',
                player=player,
                session_start=session_start,
                session_end=session_end,
            )

        for player in list(sessions):
            session_start, session_end = sessions[player]
            if session_end is None:
                continue

            if time.time() - session_end > session_threshold:
                sessions.pop(player)
                log.debug(
                    'session_saved',
                    player=player,
                    session_start=session_start,
                    sesssion_end=session_end,
                )
                con.execute(
                    'insert into sessions (player, session_start, session_end) values (?, ?, ?)',
                    [
                        (key, session_start, session_end)
                        for key, (session_start, session_end) in sessions.items()
                    ],
                )

        await trio.sleep(60)
