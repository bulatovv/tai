"""
Main application task runner.

Initializes database and starts long-running asynchronous tasks
for data collection and reporting.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import NoReturn

import trio
import trio_asyncio

from tai.collection import collect_players, collect_sessions, collect_worlds
from tai.database import init_db
from tai.database.connector import get_connection
from tai.logging import log
from tai.reports.digest import (
    Range,
    get_date_range,
    get_digest_data,
    render_digest_report,
)
from tai.settings import settings
from tai.telegram_utils import (
    init_telegram_bot,
    send_telegram_message,
    shutdown_telegram_bot,
)


async def weekly_players_collection(db_path: str, temp_db_path: str) -> NoReturn:
    """Periodically collect players data."""
    while True:
        with get_connection(db_path) as con:
            try:
                last_collection_time = con.sql(
                    'select max(snapshot_time) from players'
                ).fetchone()[0]
            except Exception:
                last_collection_time = None

        if last_collection_time:
            # Calculate the time of the next collection
            next_collection_time = last_collection_time + timedelta(weeks=1)
            now = datetime.now()
            wait_for = (next_collection_time - now).total_seconds()

            if wait_for > 0:
                log.info('weekly_players_collection_sleeping', wait_for_seconds=wait_for)
                await trio.sleep(wait_for)

        try:
            await collect_players(db_path, temp_db_path)

        except Exception as e:
            log.error('players_collection_failed', error=e)


async def daily_digest_task() -> NoReturn:
    """Periodically generate and post daily digests."""
    while True:
        now = datetime.now()
        today_at_2359 = now.replace(hour=23, minute=59, second=0, microsecond=0)

        if now > today_at_2359:
            # It's past 23:59, so schedule for tomorrow
            next_run_time = today_at_2359 + timedelta(days=1)
        else:
            # Schedule for today
            next_run_time = today_at_2359

        wait_for = (next_run_time - now).total_seconds()
        log.info('daily_digest_task_sleeping', wait_for_seconds=wait_for)
        await trio.sleep(wait_for)

        try:
            today = datetime.now().date()
            range_enum = Range.day  # Default

            # is it end of year?
            if today.month == 12 and today.day == 31:
                range_enum = Range.year
            # is it end of month?
            elif (today + timedelta(days=1)).day == 1:
                range_enum = Range.month
            # is it end of week (sunday)?
            elif today.weekday() == 6:
                range_enum = Range.week

            start, end = get_date_range(range_enum, today.isoformat())

            log.info('daily_digest_report_generation_started', range=range_enum.value)
            data = get_digest_data(start, end)
            _active_players_df, popular_worlds_df, _peak_online = data

            if not popular_worlds_df.is_empty():
                report = render_digest_report(range_enum, start, end, data)

                try:
                    await send_telegram_message(
                        report, settings.telegram_channel_id, send_as=settings.telegram_bot_id
                    )
                    log.info('daily_digest_sent_to_telegram', range=range_enum.value)

                except Exception as e_telegram:
                    log.error('daily_digest_telegram_send_failed', error=e_telegram)

            else:
                log.info('daily_digest_skipped_no_popular_worlds', range=range_enum.value)

        except Exception as e:
            log.error('daily_digest_task_failed', error=e)


async def main():
    """Module entrypoint"""
    log.info('tai_started')

    await init_telegram_bot()

    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    sessions_db_path = str(data_dir / 'sessions.db')
    players_db_path = str(data_dir / 'players.db')
    players_temp_db_path = str(data_dir / 'players_temp.db')
    online_db_path = str(data_dir / 'online.db')
    worlds_online_db_path = str(data_dir / 'worlds_online.db')
    world_sessions_db_path = str(data_dir / 'world_sessions.db')

    init_db(sessions_db_path, 'schema_sessions.sql')
    init_db(players_db_path, 'schema_players.sql')
    init_db(players_temp_db_path, 'schema_players.sql')
    init_db(online_db_path, 'schema_online.sql')
    init_db(worlds_online_db_path, 'schema_worlds_online.sql')
    init_db(world_sessions_db_path, 'schema_world_sessions.sql')

    try:
        async with trio.open_nursery() as n:
            n.start_soon(collect_sessions, sessions_db_path, online_db_path)
            n.start_soon(weekly_players_collection, players_db_path, players_temp_db_path)
            n.start_soon(collect_worlds, worlds_online_db_path, world_sessions_db_path)
            n.start_soon(daily_digest_task)

    except Exception as e:
        log.critical('main_nursery_crashed', error=e)

    finally:
        # Clean up the Bot session
        await shutdown_telegram_bot()
        log.info('tai_shutting_down')


if __name__ == '__main__':
    try:
        trio_asyncio.run(main)
    except KeyboardInterrupt:
        log.info('tai_shutdown_requested_by_user')
