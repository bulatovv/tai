# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "plotly",
#   "numpy",
#   "pandas",
# ]
# ///

import os

import duckdb
import plotly.express as px

# Get the directory of the script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the relative path to the database
db_path = os.path.join(script_dir, '..', 'data', 'sessions.db')

# Connect to the database
con = duckdb.connect(db_path)

# SQL query to calculate the average time between sessions for each player on the same day
query = """
WITH session_times AS (
    SELECT
        player,
        session_start,
        LAG(session_end, 1) OVER (PARTITION BY player ORDER BY session_start) AS prev_session_end
    FROM sessions
),
time_diffs AS (
    SELECT
        player,
        (epoch(session_start) - epoch(prev_session_end)) / 60.0 AS time_diff_minutes
    FROM session_times
    WHERE prev_session_end IS NOT NULL AND DATE(session_start) = DATE(prev_session_end)
)
SELECT
    player,
    AVG(time_diff_minutes) AS avg_time_diff
FROM time_diffs
GROUP BY player;
"""

# Execute the query and fetch the results into a pandas DataFrame
df = con.execute(query).fetchdf()

# Create the histogram
fig = px.histogram(
    df,
    x='avg_time_diff',
    title='Average Time Between Player Sessions on the Same Day (minutes)',
)
fig.show()

# Close the database connection
con.close()
