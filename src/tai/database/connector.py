import duckdb
from tenacity import retry, stop_after_attempt, wait_fixed


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Get a duckdb connection with retry."""
    return duckdb.connect(db_path)
