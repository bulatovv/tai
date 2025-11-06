import re
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Annotated

import duckdb
import numpy as np
import polars as pl
import typer
from rich.console import Console
from tlds import tld_set

app = typer.Typer()

console = Console()

PLAYERS_BLACKLIST = [
    'KrystallBot',
    'chrono.czo.ooo',
    'ChepoBot',
    'botWivar',
]


def is_safe(text: str | None) -> bool:
    """Checks if a string is safe based on a set of rules."""
    if not text:
        return True

    processed_text = ''.join(text.lower().split())

    if 'http://' in processed_text or 'https://' in processed_text:
        return False

    if 't.me/' in processed_text:
        return False

    if re.search(r'@[a-zA-Z0-9_]+', processed_text):
        return False

    match = re.search(r'([a-z0-9-]{1,}\.){1,}[a-z0-9-]{2,}', processed_text)
    if match:
        potential_domain = match.group(0)
        parts = potential_domain.split('.')
        if parts[-1] in tld_set:
            return False

    return not ('rp' in processed_text and 'sex' in processed_text)


class Range(str, Enum):
    """Enumeration for the time range of the digest."""

    day = 'day'
    week = 'week'
    month = 'month'
    year = 'year'


month_names_ru_genitive = {
    1: 'января',
    2: 'февраля',
    3: 'марта',
    4: 'апреля',
    5: 'мая',
    6: 'июня',
    7: 'июля',
    8: 'августа',
    9: 'сентября',
    10: 'октября',
    11: 'ноября',
    12: 'декабря',
}

month_names_ru_nominative = {
    1: 'январь',
    2: 'февраль',
    3: 'март',
    4: 'апреля',
    5: 'май',
    6: 'июнь',
    7: 'июль',
    8: 'август',
    9: 'сентября',
    10: 'октября',
    11: 'ноября',
    12: 'декабрь',
}


def format_date_ru(date_obj: date) -> str:
    """Formats a date object into 'day month_name' in Russian."""
    day = date_obj.day
    month = month_names_ru_genitive[date_obj.month]
    return f'{day} {month}'


def pluralize_players(count: int | None) -> str:
    """Returns the correct plural form of the word 'игрок' in Russian."""
    if count is None:
        return 'игроков'
    if count % 10 == 1 and count % 100 != 11:
        return 'игрок'
    elif 2 <= count % 10 <= 4 and (count % 100 < 10 or count % 100 >= 20):
        return 'игрока'
    else:
        return 'игроков'


def format_duration_rounded(hours: float | None) -> str:
    """Formats duration in hours into a human-readable string in Russian."""
    if not hours or hours < 0:
        return 'неизвестно'

    total_minutes = int(hours * 60)
    rounded_minutes = 5 * round(total_minutes / 5)

    if rounded_minutes < 5:
        return 'меньше 5 минут'

    if rounded_minutes < 60:
        return f'{rounded_minutes} минут'

    h = rounded_minutes // 60
    m = rounded_minutes % 60

    hours_str = 'час'
    if 1 < h < 5:
        hours_str = 'часа'
    elif h >= 5:
        hours_str = 'часов'

    minutes_str = 'минут'
    if m % 10 == 1 and m != 11:
        minutes_str = 'минута'
    elif 1 < m % 10 < 5 and m not in [12, 13, 14]:
        minutes_str = 'минуты'

    if m == 0:
        return f'{h} {hours_str}'
    else:
        return f'{h} {hours_str} {m} {minutes_str}'


def get_date_range(range_enum: Range, start_date_str: str | None):
    """Calculate the start and end dates for a given time range."""
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


def get_most_active_players(
    con: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
    top_n: int = 3,
):
    """Get the most active players by total session duration."""

    query = """
        SELECT
            player,
            SUM(epoch(session_end) - epoch(session_start)) / 3600.0 AS total_duration_hours
        FROM db_sessions.sessions
        WHERE session_start >= ? AND session_start < ?
        GROUP BY player
        ORDER BY total_duration_hours DESC
    """

    params = [start_date, end_date]

    df = con.execute(query, params).pl()

    if PLAYERS_BLACKLIST:
        df = df.filter(~pl.col('player').is_in(PLAYERS_BLACKLIST))

    df = df.filter(pl.col('player').map_elements(is_safe, return_dtype=pl.Boolean))

    return df.limit(top_n)


def get_most_popular_worlds(
    con: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
    top_n: int = 5,
):
    """Get the most popular worlds based on an area-under-curve (AUC) score."""
    query = """
        SELECT
            online.name,
            online.players,
            (epoch(online.saved_at) - epoch(sessions.session_start)) / 3600.0 AS time_elapsed,
            (epoch(sessions.session_end) - epoch(sessions.session_start)) / 3600.0 AS session_length_hours
        FROM db_worlds_online.worlds_online AS online
        JOIN db_world_sessions.world_sessions AS sessions ON online.name = sessions.name AND online.saved_at BETWEEN sessions.session_start AND sessions.session_end
        WHERE sessions.session_start >= ? AND sessions.session_start < ?
        ORDER BY online.name, time_elapsed
    """
    df = con.execute(query, [start_date, end_date]).pl()
    if df.height == 0:
        return pl.DataFrame()
    return (
        df.group_by('name')
        .agg(
            pl.col('players').alias('players_list'),
            pl.col('time_elapsed').alias('time_elapsed_list'),
            pl.col('players').max().alias('peak_players'),
            pl.col('session_length_hours').first().alias('session_length'),
        )
        .with_columns(
            pl.struct(['players_list', 'time_elapsed_list'])
            .map_elements(
                lambda s: np.trapezoid(
                    y=[0] + s['players_list'], x=[0] + s['time_elapsed_list']
                ),
                return_dtype=pl.Float64,
            )
            .alias('auc')
        )
        .select('name', 'auc', 'peak_players', 'session_length')
        .filter(
            (pl.col('peak_players') >= 5)
            & (pl.col('auc') >= 0.6)
            & (pl.col('session_length') >= (20 / 60.0))
        )
        .filter(pl.col('name').map_elements(is_safe, return_dtype=pl.Boolean))
        .sort('auc', descending=True)
        .limit(top_n)
    )


def get_peak_server_online(con: duckdb.DuckDBPyConnection, start_date: date, end_date: date):
    """Get the peak number of players online within a given date range."""
    query = 'SELECT MAX(online_count) as peak_online FROM db_online.online WHERE queried_at >= ? AND queried_at < ?'
    result = con.execute(query, [start_date, end_date]).fetchone()
    return result[0] if result else 0


def get_digest_data(
    start_date: date,
    end_date: date,
) -> tuple[pl.DataFrame, pl.DataFrame, int | None]:
    """Fetches digest data from the database."""
    with duckdb.connect() as con:
        con.execute("ATTACH 'data/sessions.db' AS db_sessions (READ_ONLY)")
        con.execute("ATTACH 'data/worlds_online.db' AS db_worlds_online (READ_ONLY)")
        con.execute("ATTACH 'data/world_sessions.db' AS db_world_sessions (READ_ONLY)")
        con.execute("ATTACH 'data/online.db' AS db_online (READ_ONLY)")

        active_players_df = get_most_active_players(con, start_date, end_date)
        popular_worlds_df = get_most_popular_worlds(con, start_date, end_date)
        peak_online = get_peak_server_online(con, start_date, end_date)

        return active_players_df, popular_worlds_df, peak_online


def render_digest_report(
    range_enum: Range,
    start_date: date,
    end_date: date,
    data: tuple[pl.DataFrame, pl.DataFrame, int | None],
) -> str:
    """Renders the digest report as a markdown string."""
    active_players_df, popular_worlds_df, peak_online = data

    if range_enum == Range.day:
        title = f'**Дайджест за {format_date_ru(start_date)}**'
    elif range_enum == Range.week:
        title = f'**Дайджест за неделю ({format_date_ru(start_date)} - {format_date_ru(end_date - timedelta(days=1))})**'
    elif range_enum == Range.month:
        title = f'**Дайджест за {month_names_ru_nominative[start_date.month]}**'
    elif range_enum == Range.year:
        title = f'**Дайджест за {start_date.year} год**'

    report = [title]

    report.append('\n**🏆 Самые активные игроки**')
    if not active_players_df.is_empty():
        for i, row in enumerate(active_players_df.iter_rows(named=True)):
            player_name = row['player']
            duration = round(row['total_duration_hours'], 1)
            emoji = ''
            if i == 0:
                emoji = '🥇 '
            elif i == 1:
                emoji = '🥈 '
            elif i == 2:
                emoji = '🥉 '
            report.append(f'{emoji}`{player_name}`: {duration} часов')
    else:
        report.append('Нет данных.')

    report.append('\n**🌍 Самые популярные миры**')
    if not popular_worlds_df.is_empty():
        for row in popular_worlds_df.iter_rows(named=True):
            world_name = row['name']
            peak_players = row['peak_players']
            session_length = row['session_length']
            auc = row['auc']

            emoji = ''
            if (
                peak_players
                and session_length
                and auc
                and peak_players >= 8
                and session_length >= 1.5
                and auc >= 5.8
            ):
                emoji = '🔥 '

            formatted_session_length = format_duration_rounded(session_length)
            report.append(
                f'\n{emoji}`{world_name}`\n  👥 Пик: {peak_players} {pluralize_players(peak_players)}\n  ⏳ Длительность: {formatted_session_length}'
            )
    else:
        report.append('Нет данных.')

    report.append(f'\n**🚀 Пиковый онлайн:** {peak_online} {pluralize_players(peak_online)}')

    return '\n'.join(report)


@app.command()
def main(
    range_enum: Annotated[Range, typer.Option(help='Time range for the digest.')] = Range.day,
    start_date_str: Annotated[
        str | None,
        typer.Option(help='Start date in YYYY-MM-DD format. Defaults to current date.'),
    ] = None,
):
    """Generates a digest of server activity."""
    start, end = get_date_range(range_enum, start_date_str)

    try:
        data = get_digest_data(start, end)
        report = render_digest_report(range_enum, start, end, data)
        console.print(report)

    except duckdb.IOException as e:
        console.print(f'[red]Error connecting to database: {e}[/red]')
        raise typer.Exit(code=1) from e
    except Exception as e:
        console.print(f'[red]An error occurred: {e}[/red]')
        raise typer.Exit(code=1) from e


if __name__ == '__main__':
    app()
