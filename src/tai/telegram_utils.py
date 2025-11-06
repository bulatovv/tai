"""
Telegram Bot Utilities

Handles formatting and sending messages to the Telegram API
using 'aiogram' (for trio compatibility) and 'telegramify-markdown'.
"""

import telegramify_markdown
import trio
import trio_asyncio
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError

from tai.logging import log
from tai.settings import settings

# This bot instance will be initialized in main()
_bot: Bot | None = None


async def init_telegram_bot():
    """
    Initializes the global Bot instance.

    Must be called once before send_telegram_message.
    """
    global _bot
    # The user stated that telegram_bot_token is always present, so no check needed here.

    # Set the default parse mode to MARKDOWN_V2, as that's what telegramify_markdown outputs
    properties = DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2)
    _bot = Bot(token=settings.telegram_bot_token, default=properties)

    try:
        # Test the connection and get bot info
        await trio_asyncio.aio_as_trio(_bot.get_me())
        log.info('telegram_bot_initialized_successfully')
    except TelegramAPIError as e:
        log.error('telegram_bot_init_failed', error=e.message)
        _bot = None  # Reset on failure
        raise  # Raise the exception as requested by the user


async def shutdown_telegram_bot():
    """
    Closes the bot's session.

    Must be called once at application shutdown.
    """
    global _bot
    if _bot:
        await trio_asyncio.aio_as_trio(_bot.session.close())
        _bot = None


async def send_telegram_message(message_text: str, channel_id: str):
    """
    Sends a message to a specified Telegram channel.

    The message is automatically converted from standard Markdown to
    Telegram-compatible MarkdownV2.

    :param message_text: The raw text (with standard markdown) to send.
    :param channel_id: The target channel ID.
    """
    if _bot is None:
        log.error('telegram_send_skipped', reason='Bot is not initialized or failed to init.')
        # As per user instruction, raise an error if bot is not initialized
        raise RuntimeError('Telegram bot is not initialized.')

    max_retry_attempts = 5
    initial_retry_delay = 2  # Start with a 2-second delay

    formatted_text = telegramify_markdown.markdownify(message_text)

    for retry_attempt in range(1, max_retry_attempts + 1):
        try:
            await trio_asyncio.aio_as_trio(
                _bot.send_message(
                    chat_id=channel_id,
                    text=formatted_text,
                    # parse_mode is already set in DefaultBotProperties
                )
            )
            log.info('telegram_message_sent_successfully', channel_id=channel_id)
            return  # Message sent, exit retry loop
        except TelegramAPIError as e:
            delay = initial_retry_delay * (2 ** (retry_attempt - 1))
            log.warning(
                'telegram_send_failed_api_error',
                retry=retry_attempt,
                of=max_retry_attempts,
                waiting_for=delay,
                error=e.message,
                channel_id=channel_id,
            )
            await trio.sleep(delay)
        except Exception as e:
            delay = initial_retry_delay * (2 ** (retry_attempt - 1))
            log.warning(
                'telegram_send_failed_unknown_error',
                retry=retry_attempt,
                of=max_retry_attempts,
                waiting_for=delay,
                error=e,
                channel_id=channel_id,
            )
            await trio.sleep(delay)

    # If all retries fail, raise an exception
    msg = f'Failed to send Telegram message after {max_retry_attempts} attempts.'
    raise RuntimeError(msg)
