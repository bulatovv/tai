# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "pandas",
#   "plotly",
# ]
# ///

from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go

DB_PATH = Path(__file__).parent.parent / 'data' / 'players.db'


def get_connection():
    """Establishes a connection to the DuckDB database."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def get_name_segments(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Segments players by name format (RP vs. non-RP)."""
    query = """
        SELECT
            CASE
                WHEN regexp_matches(login, '^[A-Z][a-z]+_[A-Z][a-z]+$') THEN 'RP Names'
                ELSE 'Not RP Names'
            END AS segment,
            COUNT(*) AS count
        FROM
            players
        GROUP BY
            segment;
    """
    return con.execute(query).fetchdf()


def plot_segments(df: pd.DataFrame):
    """Generates and displays a pie chart of the segments."""
    fig = go.Figure(data=[go.Pie(labels=df['segment'], values=df['count'], hole=0.3)])
    fig.update_layout(title_text='Player Name Segmentation (RP vs. Not RP)')
    fig.show()


def main():
    """Main function to get, print, and plot name segments."""
    con = get_connection()
    segments_df = get_name_segments(con)
    con.close()

    print('Player Name Segmentation:')
    print(segments_df.to_string(index=False))

    plot_segments(segments_df)


if __name__ == '__main__':
    main()
