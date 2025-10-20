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


def plot_data(df: pd.DataFrame, title: str, y_axis_title: str):
    """Generates and displays a line plot."""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df['period'], y=df['avg_age_years'], mode='lines', line=dict(color='red', width=3)
        )
    )
    fig.update_layout(
        title_text=title,
        title_font=dict(size=24),
        xaxis_title='Год',
        yaxis_title=y_axis_title,
        xaxis=dict(title_font=dict(size=18)),
        yaxis=dict(title_font=dict(size=18)),
        hovermode='x unified',
        template='plotly_white',
        width=800,
        height=600,
        margin=dict(t=120, r=40),
    )
    fig.update_yaxes(showgrid=False)
    fig.update_xaxes(showgrid=True)

    # Add annotations for specific ages
    annotation_ages = [1, 2, 3, 4]
    for age in annotation_ages:
        # Find the period where the age is closest to the target age
        closest_index = (df['avg_age_years'] - age).abs().idxmin()
        fig.add_annotation(
            x=df['period'][closest_index],
            y=df['avg_age_years'][closest_index],
            text=f'~{age} г.',
            showarrow=True,
            arrowhead=1,
            ax=-40,
            ay=-40,
            font=dict(size=16),
        )

    fig.show()


def get_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Calculates the average account age in years per month."""

    query = """


    WITH


    -- Generate a series of months from the earliest registration date to the current date


    months AS (


        SELECT


            CAST(date_trunc('month', dd.generate_series) AS date) AS period_start


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


    -- Calculate player activity and tenor


    player_activity AS (


        SELECT


            p.id,


            p.regdate,


            COALESCE(p.lastlogin, p.regdate) AS last_activity


        FROM players p


        WHERE p.regdate >= '2018-01-01'


    ),


    -- Calculate age for each player for each period they were active in


    player_ages AS (


        SELECT


            strftime(per.period_start, '%Y-%m') AS period,


            date_diff('month', pa.regdate, per.period_end) AS account_age_months


        FROM periods per


        JOIN player_activity pa ON pa.regdate <= per.period_end


        WHERE pa.last_activity > pa.regdate + interval '1 month'


    )


    -- Main query to calculate average account age for each period


    SELECT


        period,


        AVG(account_age_months) / 12.0 AS avg_age_years


    FROM player_ages


    GROUP BY period


    ORDER BY period;


    """

    return con.execute(query).fetchdf()


def main():
    """Main function to generate and display the plot."""
    con = get_connection()
    df = get_data(con)
    plot_data(
        df,
        '<b>Медианный возраст активных аккаунтов<br>с течением времени</b>',
        'Медианный возраст',
    )
    con.close()


if __name__ == '__main__':
    main()
