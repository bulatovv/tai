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


def get_cohort_sizes(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Calculates the size of each yearly cohort."""
    query = """
        SELECT
            strftime(regdate, '%Y') AS year,
            COUNT(*) AS cohort_size
        FROM
            players
        WHERE
            regdate >= '2018-01-01'
        GROUP BY
            year
        ORDER BY
            year;
    """
    return con.execute(query).fetchdf()


def plot_cohort_sizes(df: pd.DataFrame):
    """Generates and displays a bar plot of cohort sizes."""
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df['year'],
            y=df['cohort_size'],
            text=df['cohort_size'],
            textposition='auto',
        )
    )
    fig.update_layout(
        title='Yearly Cohort Sizes (Number of Registered Accounts)',
        xaxis_title='Year',
        yaxis_title='Number of Registered Accounts',
    )
    fig.show()


def main():
    """Main function to get, print, and plot cohort sizes."""
    con = get_connection()
    cohort_sizes_df = get_cohort_sizes(con)
    con.close()

    print('Yearly Cohort Sizes (Number of Registered Accounts):')
    print(cohort_sizes_df.to_string(index=False))

    plot_cohort_sizes(cohort_sizes_df)


if __name__ == '__main__':
    main()
