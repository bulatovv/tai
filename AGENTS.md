# GEMINI.md

## Project Overview

This project, named "tai", is a Python application designed to collect and store data from a San Andreas Multiplayer (SA-MP) server and its associated web APIs. It functions as a data pipeline, tracking player activity, server status, and in-game world data. The project's own description is: "TRAINING SANDBOX server data collection service for analytics."

The application collects the following data:

*   **Player Data:** A weekly snapshot of all registered players, including their login information, registration date, and other metadata.
*   **Player Sessions:** Individual player gaming sessions, including the start and end times.
*   **World Data:** Information about the game worlds, including the number of players, whether the world is static, and when the data was saved.
*   **World Sessions:** The start and end times of each game world's existence.
*   **Online Player Count:** The number of players online at any given time.

The core technologies used are:

*   **Python 3.10+:** The primary programming language.
*   **Trio:** An asynchronous I/O framework for concurrent operations.
*   **DuckDB:** An in-process SQL OLAP database for storing and querying the collected data.
*   **Polars:** A fast DataFrame library for data manipulation.
*   **HTTPX:** A modern, asynchronous HTTP client for interacting with web APIs.
*   **Pydantic:** For data validation and settings management.
*   **Ruff:** For code formatting and linting.

The application is structured to run as a persistent service, continuously collecting data in the background. It uses a modular design, with separate components for data collection, database management, and configuration.

## Data Collection

The application runs three main data collection processes concurrently:

*   `collect_sessions`: This process continuously monitors the SA-MP server for player connections and disconnections. It tracks individual player gaming sessions and saves them to the `sessions.db` database.
*   `weekly_players_collection`: This process runs once a week, fetching a complete list of all registered players from the training server's web API. It stores this data in the `players.db` database.
*   `collect_worlds`: This process continuously fetches data about the game worlds from the "chrono" web API. It tracks the number of players in each world, as well as the start and end times of each world's session. This data is stored in the `worlds_online.db` and `world_sessions.db` databases.

## Building and Running

### Dependencies

The project uses `uv` for dependency management. To install the required packages, run:

```bash
uv sync --all-groups
```

### Configuration

The application requires a `.env` file in the root directory to store configuration settings. An example is provided in `.env.example`. The key settings include:

*   `LOG_LEVEL`: The logging level (e.g., `INFO`, `DEBUG`).
*   `TRAINING_HOST`: The hostname or IP address of the SA-MP server.
*   `TRAINING_PORT`: The port of the SA-MP server.
*   `TRAINING_API_BASE_URL`: The base URL of the training server's web API.
*   `CHRONO_LOGIN`: The login for the "chrono" API.
*   `CHRONO_TOKEN`: The authentication token for the "chrono" API.

### Running the Application

The main entry point for the application is `src/tai/__main__.py`. To run the application, execute the following command from the project root:

```bash
python -m tai
```

This will start the data collection processes, which will run indefinitely until the application is terminated.

### Testing

There are no explicit tests included in the project.

## Database

The project uses DuckDB for its database. The database is split into several files, each with its own schema. The schema definitions are located in `.sql` files in the `src/tai/database` directory.

The database schemas are as follows:

*   `schema_players.sql`: Defines the `players` table, which stores a weekly snapshot of all registered players.
*   `schema_sessions.sql`: Defines the `sessions` table, which stores individual player gaming sessions.
*   `schema_worlds_online.sql`: Defines the `worlds_online` table, which stores information about the game worlds, including the number of players.
*   `schema_world_sessions.sql`: Defines the `world_sessions` table, which stores the start and end times of each game world's session.
*   `schema_online.sql`: Defines the `online` table, which stores the number of players online at a given time.

## Development Conventions

*   **Asynchronous Code:** The project uses the `trio` library for asynchronous programming. All I/O operations should be performed asynchronously to avoid blocking the event loop.
*   **Configuration:** Application settings are managed through a `Settings` class in `src/tai/settings.py` using `pydantic-settings`. All configuration should be defined in this file and loaded from environment variables or a `.env` file.
*   **Linting and Formatting:** The project uses `ruff` for code linting and formatting. The configuration is defined in the `pyproject.toml` file. It is recommended to run `ruff check --fix` and `ruff format` before committing any changes.
*   **Pre-commit Hooks:** The project includes a `.pre-commit-config.yaml` file, which suggests that pre-commit hooks are used to enforce code quality. To install the hooks, run `pre-commit install`.

## Ad-hoc Scripts

All throw-away scripts, for example for explorative data analysis, must be placed into the `ad-hoc` directory. These scripts must have all required dependencies included into PEP 723 inline script metadata. For example:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "plotly",
#   "numpy",
#   "pandas",
# ]
# ///
```
