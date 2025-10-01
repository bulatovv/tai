from pathlib import Path

import duckdb

from tai.logging import log


def init_db(db_path: str, schema_path: str):
    """Initialize DuckDB database with schema from a given SQL file."""
    con = duckdb.connect(db_path)
    here = Path(__file__).resolve().parent

    with (here / schema_path).open() as file:
        con.sql(file.read())
    con.close()
    log.info('database_initialized', db_path=db_path, schema=schema_path)
