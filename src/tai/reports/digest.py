


import typer
from datetime import datetime, timedelta
from enum import Enum
from rich.console import Console
from rich.table import Table

import duckdb
import numpy as np
import polars as pl

app = typer.Typer()
console = Console()

class Range(str, Enum):
    day = "day"
    week = "week"
    month = "month"
    year = "year"

def get_date_range(range_enum: Range, start_date_str: str):
    if start_date_str:
        start_date = datetime.fromisoformat(start_date_str).date()
    else:
        start_date = datetime.now().date()

    if range_enum == Range.day:
        end_date = start_date + timedelta(days=1)
    elif range_enum == Range.week:
        start_date = start_date - timedelta(days=start_date.weekday())
        end_date = start_date + timedelta(weeks=1)
    elif range_enum == Range.month:
        start_date = start_date.replace(day=1)
        next_month = start_date.replace(day=28) + timedelta(days=4)
        end_date = next_month - timedelta(days=next_month.day - 1)
    elif range_enum == Range.year:
        start_date = start_date.replace(month=1, day=1)
        end_date = start_date.replace(year=start_date.year + 1)
    
    return start_date, end_date

def get_most_active_players(con: duckdb.DuckDBPyConnection, start_date: datetime.date, end_date: datetime.date, top_n: int = 10):
    query = """
        SELECT
            player,
            SUM(epoch(session_end) - epoch(session_start)) / 3600.0 AS total_duration_hours
        FROM db_sessions.sessions
        WHERE session_start >= ? AND session_start < ?
        GROUP BY player
        ORDER BY total_duration_hours DESC
        LIMIT ?
    """
    df = con.execute(query, [start_date, end_date, top_n]).pl()
    return df

def get_most_popular_worlds(con: duckdb.DuckDBPyConnection, start_date: datetime.date, end_date: datetime.date, top_n: int = 10):
    query = """
        SELECT
            online.name,
            online.players,
            (epoch(online.saved_at) - epoch(sessions.session_start)) / 3600.0 AS time_elapsed,
            (epoch(sessions.session_end) - epoch(sessions.session_start)) / 3600.0 AS session_length_hours
        FROM db_worlds_online.worlds_online AS online
        JOIN db_world_sessions.world_sessions AS sessions ON online.name = sessions.name AND online.saved_at BETWEEN sessions.session_start AND sessions.session_end
        WHERE sessions.session_start >= ? AND sessions.session_start < ? AND online.players >= 5
        ORDER BY online.name, time_elapsed
    """
    df = con.execute(query, [start_date, end_date]).pl()

    if df.height == 0:
        return pl.DataFrame()

    auc_scores = (
        df.group_by("name")
        .agg(
            pl.col("players").alias("players_list"),
            pl.col("time_elapsed").alias("time_elapsed_list"),
            pl.col("players").max().alias("peak_players"),
            pl.col("session_length_hours").first().alias("session_length"),
        )
        .with_columns(
            pl.struct(["players_list", "time_elapsed_list"])
            .map_elements(
                lambda s: np.trapezoid(y=[0] + s["players_list"], x=[0] + s["time_elapsed_list"]),
                return_dtype=pl.Float64,
            )
            .alias("auc")
        )
        .select("name", "auc", "peak_players", "session_length")
        .sort("auc", descending=True)
        .limit(top_n)
    )
    return auc_scores

def get_peak_server_online(con: duckdb.DuckDBPyConnection, start_date: datetime.date, end_date: datetime.date):
    query = """
        SELECT MAX(online_count) as peak_online
        FROM db_online.online
        WHERE queried_at >= ? AND queried_at < ?
    """
    result = con.execute(query, [start_date, end_date]).fetchone()
    return result[0] if result else 0

@app.command()
def main(
    range: Range = typer.Option(Range.day, help="Time range for the digest."),
    start_date: str = typer.Option(None, help="Start date in YYYY-MM-DD format. Defaults to current date."),
):
    """
    Generates a digest of server activity.
    """
    start, end = get_date_range(range, start_date)
    console.print(f"[bold]Digest for {range.value} from {start} to {end}[/bold]")

    try:
        with duckdb.connect() as con:
            con.execute("ATTACH 'data/sessions.db' AS db_sessions (READ_ONLY)")
            con.execute("ATTACH 'data/worlds_online.db' AS db_worlds_online (READ_ONLY)")
            con.execute("ATTACH 'data/world_sessions.db' AS db_world_sessions (READ_ONLY)")
            con.execute("ATTACH 'data/online.db' AS db_online (READ_ONLY)")

            # Most Active Players
            console.print("\n[bold]Most Active Players[/bold]")
            active_players_df = get_most_active_players(con, start, end)
            table = Table(show_header=True, header_style="bold magenta")
            for col in active_players_df.columns:
                table.add_column(col)
            for row in active_players_df.iter_rows():
                table.add_row(*[str(item) for item in row])
            console.print(table)

            # Most Popular Worlds
            console.print("\n[bold]Most Popular Worlds[/bold]")
            popular_worlds_df = get_most_popular_worlds(con, start, end)
            table = Table(show_header=True, header_style="bold magenta")
            for col in popular_worlds_df.columns:
                table.add_column(col)
            for row in popular_worlds_df.iter_rows():
                table.add_row(*[str(item) for item in row])
            console.print(table)

            # Peak Server Online
            console.print("\n[bold]Peak Server Online[/bold]")
            peak_online = get_peak_server_online(con, start, end)
            console.print(f"{peak_online} players")

    except duckdb.IOException as e:
        console.print(f"[red]Error connecting to database: {e}[/red]")
    except Exception as e:
        console.print(f"[red]An error occurred: {e}[/red]")

if __name__ == "__main__":
    app()