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

# SQL query to calculate the average number of sessions per player per day
query = """
WITH daily_sessions AS (
    SELECT
        player,
        CAST(session_start AS DATE) AS session_date,
        COUNT(*) AS session_count
    FROM sessions
    GROUP BY player, CAST(session_start AS DATE)
)
SELECT
    player,
    AVG(session_count) AS avg_sessions
FROM daily_sessions
GROUP BY player;
"""

# Execute the query and fetch the results into a pandas DataFrame
df = con.execute(query).fetchdf()

# Create the histogram
fig = px.histogram(df, x='avg_sessions', title='Average Sessions per Player per Day')
fig.show()

# Close the database connection
con.close()
