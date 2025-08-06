import duckdb
import trio

from tai.collection import collect_sessions
from tai.database import init_db


async def main():
    """Module entrypoint"""
    init_db()
    with duckdb.connect('database.db') as con:
        async with trio.open_nursery() as n:
            n.start_soon(collect_sessions, con)


trio.run(main)
