from __future__ import annotations

import asyncio
import atexit
import os
from datetime import datetime, timedelta
from typing import Any, Optional, Iterable, AsyncGenerator

import aiohttp
import codetiming
import tinkoff.invest as ti

import logger

asset_types: list[str] = [
    'bond',
    'currency',
    'etf',
    'future',
    'option',
    'share',
]

# Getters of InstrumentsService
_instrument_getters = {
    'bond': 'bonds',
    'currency': 'currencies',
    'etf': 'etfs',
    'future': 'futures',
    'option': 'options',
    'share': 'shares',
}

_token = os.environ['INVEST_TOKEN']
_history_headers = {'Authorization': 'Bearer ' + _token}
# TODO initialize
_session: Optional[aiohttp.ClientSession] = None
_history_limit = 1
_history_limit_reset = datetime.now() + timedelta(minutes=1)
_history_request_queue = asyncio.Queue()


async def get_instruments(_asset_types: Optional[Iterable[str]] = None) -> AsyncGenerator[tuple[str, Any]]:
    """ Download instrument info.

    :return: Pairs of instrument info and API response
    """
    async def instrument_get_task(asset_type: str, getter_name: str) -> tuple[str, Any]:
        getter = getattr(client.instruments, getter_name)
        count = 0
        with codetiming.Timer(initial_text=f"Requesting {getter_name}...",
                              text=lambda elapsed: f"Received {count} {getter_name} in {elapsed:.2f}s.",
                              logger=logger.debug):
            response = await getter(instrument_status=ti.schemas.InstrumentStatus.INSTRUMENT_STATUS_ALL)
            count = len(response.instruments)
        return asset_type, response

    # Task launcher
    getters = (getter for getter in _instrument_getters.items() if not _asset_types or getter[0] in _asset_types)
    async with ti.AsyncClient(_token) as client:
        tasks = [asyncio.create_task(instrument_get_task(asset_type, getter_name)) for asset_type, getter_name in getters]
        for task in asyncio.as_completed(tasks):
            yield await task


def _close():
    # Clean-up at exit.
    if _session:
        asyncio.run(_session.close())
        logger.debug("aiohttp session closed.")

atexit.register(_close)
