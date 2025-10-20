# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "plotly",
#   "pandas",
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


def plot_data(df: pd.DataFrame):
    """Generates and displays a stacked area plot."""
    fig = go.Figure()

    cohorts = sorted(df['cohort_year'].unique())

    for cohort in cohorts:
        cohort_df = df[df['cohort_year'] == cohort]
        fig.add_trace(
            go.Scatter(
                x=cohort_df['period'],
                y=cohort_df['player_count'],
                name=cohort,
                stackgroup='one',
                mode='lines',
            )
        )

    fig.update_layout(
        title_text='<b>Распределение игроков по когортам с течением времени</b>',
        xaxis_title='Период',
        yaxis_title='Количество игроков',
        hovermode='x unified',
        template='plotly_white',
    )
    fig.show()


def get_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Calculates the number of players per registration cohort for each month."""
    query = """
    WITH
    -- Generate a series of months
    months AS (
        SELECT CAST(date_trunc('month', dd.generate_series) AS date) AS period_start
        FROM generate_series(
            CAST((SELECT MIN(regdate) FROM players WHERE regdate >= '2018-01-01') AS TIMESTAMP WITH TIME ZONE),
            now(),
            interval '1 month'
        ) AS dd
    ),
    -- Define the period end for each month
    periods AS (
        SELECT
            period_start,
            period_start + interval '1 month' - interval '1 day' AS period_end
        FROM months
    ),
    -- Get players with their registration year as cohort
    players_with_cohort AS (
        SELECT
            id,
            regdate,
            COALESCE(lastlogin, regdate) AS last_activity,
            strftime(regdate, '%Y') AS cohort_year
        FROM players
        WHERE regdate >= '2018-01-01'
    )
    -- Main query to count active players per cohort for each period
    SELECT
        strftime(p.period_start, '%Y-%m') AS period,
        pc.cohort_year,
        COUNT(pc.id) AS player_count
    FROM periods p
    JOIN players_with_cohort pc
        ON pc.regdate <= p.period_end AND pc.last_activity >= p.period_start
    GROUP BY period, pc.cohort_year
    ORDER BY period, pc.cohort_year;
    """
    df = con.execute(query).fetchdf()

    if not df.empty:
        last_period = df['period'].max()
        df = df[df['period'] != last_period]

    return df


def main():
    """Main function to generate and display the plot."""
    con = get_connection()
    df = get_data(con)
    plot_data(df)
    con.close()


if __name__ == '__main__':
    main()
