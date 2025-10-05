"""Logging configuration for the TAI application."""

import logging
import sys

import structlog

from tai.settings import settings

shared_processors: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt='iso'),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]

structlog.configure(
    processors=shared_processors
    + [
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

log_renderer: structlog.types.Processor
if settings.log_json_format:
    log_renderer = structlog.processors.JSONRenderer()
else:
    log_renderer = structlog.dev.ConsoleRenderer(
        exception_formatter=structlog.dev.RichTracebackFormatter(
            show_locals=settings.log_exception_include_locals
        )
    )

formatter = structlog.stdlib.ProcessorFormatter(
    foreign_pre_chain=shared_processors,
    processors=[
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        log_renderer,
    ],
)


def _demote_info_to_debug(record: logging.LogRecord) -> bool:
    # If a log record is INFO level, this function changes it to DEBUG.
    if record.levelno == logging.INFO:
        record.levelno = logging.DEBUG
        record.levelname = 'DEBUG'
    return True


log = structlog.get_logger('tai')

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.addHandler(handler)
root_logger.setLevel(settings.log_level.upper())

httpx_logger = logging.getLogger('httpx')
httpx_logger.addFilter(_demote_info_to_debug)
httpx_logger.setLevel(logging.WARNING)
