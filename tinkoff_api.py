from __future__ import annotations

import asyncio
import atexit
import os
from datetime import datetime, timedelta
from typing import Any, AsyncIterable, Optional

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
_session: Optional[aiohttp.ClientSession] = None
_history_limit = 1
_history_limit_reset = datetime.now() + timedelta(minutes=1)
_history_request_queue = asyncio.Queue()


async def get_instruments() -> AsyncIterable[tuple[str, Any]]:
    """ Download instrument info.

    :return: Pairs of instrument info and API response
    """
    async def instrument_get_task(asset_type: str, getter_name: str) -> tuple[str, Any]:
        getter = getattr(client.instruments, getter_name)
        with codetiming.Timer(initial_text=f"Requesting {getter_name}...",
                              text=lambda elapsed: f"Received {count} {getter_name} in {elapsed:.2f}s.",
                              logger=logger.debug):
            response = await getter()
            count = len(response.instruments)
        return asset_type, response

    # Task launcher
    async with ti.AsyncClient(_token) as client:
        tasks = [asyncio.create_task(instrument_get_task(asset_type, getter_name)) for asset_type, getter_name in _instrument_getters.items()]
        for task in asyncio.as_completed(tasks):
            yield await task


def _close():
    # Clean-up at exit.
    if _session:
        asyncio.run(_session.close())
        logger.debug("aiohttp session closed.")

atexit.register(_close)
