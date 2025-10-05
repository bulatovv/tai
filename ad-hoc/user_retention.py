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


def plot_retention(df: pd.DataFrame, title: str, y_axis_title='Retention Rate (%)'):
    """Generates and displays a retention plot."""
    fig = go.Figure()
    for cohort in sorted(df['cohort'].unique()):
        cohort_df = df[df['cohort'] == cohort]
        fig.add_trace(
            go.Scatter(
                x=cohort_df['day'], y=cohort_df['retention'], mode='lines', name=str(cohort)
            )
        )
    fig.update_layout(
        title=title,
        xaxis_title='Days Since Registration',
        yaxis_title=y_axis_title,
        hovermode='x unified',
    )
    fig.show()


def get_overall_retention(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Calculates overall rolling retention."""
    query = """
        WITH player_days AS (
            SELECT
                date_diff('day', regdate, COALESCE(lastlogin, regdate)) AS days_active
            FROM players
            WHERE regdate >= '2018-01-01'
        ),
        total_players AS (
            SELECT COUNT(*) AS total FROM players WHERE regdate >= '2018-01-01'
        ),
        daily_retention AS (
            SELECT
                days_active,
                COUNT(*) as active_count
            FROM player_days
            GROUP BY days_active
        ),
        rolling_retention AS (
            SELECT
                days_active as day,
                SUM(active_count) OVER (ORDER BY days_active DESC) as retained_count
            FROM daily_retention
        )
        SELECT
            'Overall' as cohort,
            day,
            (retained_count * 100.0 / (SELECT total FROM total_players)) as retention
        FROM rolling_retention
        ORDER BY day;
    """
    return con.execute(query).fetchdf()


def get_retention_by_cohort(con: duckdb.DuckDBPyConnection, period: str) -> pd.DataFrame:
    """Calculates rolling retention grouped by a specific period (year, quarter, month)."""
    cohort_format = {
        'yearly': "strftime(regdate, '%Y')",
        'quarterly': "strftime(regdate, '%Y-Q') || QUARTER(regdate)",
        'monthly': "strftime(regdate, '%Y-%m')",
    }[period]

    query = f"""
        WITH player_days AS (
            SELECT
                {cohort_format} AS cohort,
                date_diff('day', regdate, COALESCE(lastlogin, regdate)) AS days_active
            FROM players
            WHERE regdate >= '2018-01-01'
        ),
        cohort_sizes AS (
            SELECT cohort, COUNT(*) AS total FROM player_days GROUP BY cohort
        ),
        daily_retention AS (
            SELECT
                cohort,
                days_active,
                COUNT(*) as active_count
            FROM player_days
            GROUP BY cohort, days_active
        ),
        rolling_retention AS (
            SELECT
                cohort,
                days_active as day,
                SUM(active_count) OVER (PARTITION BY cohort ORDER BY days_active DESC) as retained_count
            FROM daily_retention
        )
        SELECT
            r.cohort,
            r.day,
            (r.retained_count * 100.0 / s.total) as retention
        FROM rolling_retention r
        JOIN cohort_sizes s ON r.cohort = s.cohort
        ORDER BY r.cohort, r.day;
    """
    return con.execute(query).fetchdf()


def main():
    """Main function to generate and display all retention plots."""
    con = get_connection()

    # 1. Overall Retention
    overall_df = get_overall_retention(con)
    plot_retention(overall_df, 'Overall User Retention')

    # 2. Yearly Retention
    yearly_df = get_retention_by_cohort(con, 'yearly')
    plot_retention(yearly_df, 'Yearly User Retention Cohorts')

    # 3. Quarterly Retention
    quarterly_df = get_retention_by_cohort(con, 'quarterly')
    plot_retention(quarterly_df, 'Quarterly User Retention Cohorts')

    # 4. Monthly Retention
    monthly_df = get_retention_by_cohort(con, 'monthly')
    plot_retention(monthly_df, 'Monthly User Retention Cohorts')

    con.close()


if __name__ == '__main__':
    main()
