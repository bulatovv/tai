import time
from datetime import timedelta
from pathlib import Path
from typing import NoReturn

import trio

from tai.collection import collect_players, collect_sessions, collect_worlds
from tai.database import init_db
from tai.database.connector import get_connection


async def weekly_players_collection(db_path: str, temp_db_path: str) -> NoReturn:
    """Periodically collect players data."""
    while True:
        with get_connection(db_path) as con:
            try:
                last_collection_time = con.sql(
                    'select max(snapshot_time) from players'
                ).fetchone()[0]
            except Exception:
                last_collection_time = None
        week = 604800

        if not last_collection_time:
            await collect_players(db_path, temp_db_path)
        else:
            wait_for = (
                last_collection_time + timedelta(seconds=week)
            ).timestamp() - time.time()
            if wait_for > 0:
                await trio.sleep(wait_for)

        await collect_players(db_path, temp_db_path)
        await trio.sleep(week)


async def main():
    """Module entrypoint"""
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    sessions_db_path = str(data_dir / 'sessions.db')
    players_db_path = str(data_dir / 'players.db')
    players_temp_db_path = str(data_dir / 'players_temp.db')
    online_db_path = str(data_dir / 'online.db')
    worlds_online_db_path = str(data_dir / 'worlds_online.db')
    world_sessions_db_path = str(data_dir / 'world_sessions.db')

    init_db(sessions_db_path, 'schema_sessions.sql')
    init_db(players_db_path, 'schema_players.sql')
    init_db(players_temp_db_path, 'schema_players.sql')
    init_db(online_db_path, 'schema_online.sql')
    init_db(worlds_online_db_path, 'schema_worlds_online.sql')
    init_db(world_sessions_db_path, 'schema_world_sessions.sql')

    async with trio.open_nursery() as n:
        n.start_soon(collect_sessions, sessions_db_path, online_db_path)
        n.start_soon(weekly_players_collection, players_db_path, players_temp_db_path)
        n.start_soon(collect_worlds, worlds_online_db_path, world_sessions_db_path)


trio.run(main)
