from pathlib import Path

import duckdb

from tai.logging import log


def init_db():
    """Initialize DuckDB database with schema from schema.sql file."""
    con = duckdb.connect('database.db')
    here = Path(__file__).resolve().parent

    with (here / 'schema.sql').open() as file:
        con.sql(file.read())
        log.info('database_initialized')
