"""Module for creating samp_query.Client instances."""

import samp_query

from tai.settings import settings


def create_client() -> samp_query.Client:
    """Create a new samp_query.Client instance."""
    return samp_query.Client(ip=settings.training_host, port=settings.training_port)
