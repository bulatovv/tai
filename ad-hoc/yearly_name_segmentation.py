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


def get_yearly_name_segments(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Gets the count of RP and non-RP names for each year."""
    query = """
        SELECT
            strftime(regdate, '%Y') AS year,
            CASE
                WHEN regexp_matches(login, '^[A-Z][a-z]+_[A-Z][a-z]+$') THEN 'RP Names'
                ELSE 'Not RP Names'
            END AS segment,
            COUNT(*) AS count
        FROM
            players
        WHERE
            regdate >= '2018-01-01'
        GROUP BY
            year,
            segment
        ORDER BY
            year,
            segment;
    """
    return con.execute(query).fetchdf()


def plot_yearly_segments(df: pd.DataFrame):
    """Generates and displays a grouped bar chart of the segments."""
    fig = go.Figure()

    # Get unique years and segments
    df['year'].unique()
    segments = df['segment'].unique()

    for segment in segments:
        segment_df = df[df['segment'] == segment]
        fig.add_trace(
            go.Bar(
                x=segment_df['year'],
                y=segment_df['count'],
                name=segment,
                text=segment_df['count'],
                textposition='auto',
            )
        )

    fig.update_layout(
        barmode='group',
        title='Yearly Player Name Segmentation',
        xaxis_title='Year',
        yaxis_title='Number of Registered Accounts',
    )
    fig.show()


def main():
    """Main function to get, print, and plot yearly name segments."""
    con = get_connection()
    yearly_segments_df = get_yearly_name_segments(con)
    con.close()

    print('Yearly Player Name Segmentation:')
    print(yearly_segments_df.to_string(index=False))

    plot_yearly_segments(yearly_segments_df)


if __name__ == '__main__':
    main()
