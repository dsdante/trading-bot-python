from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import contextlib
import io
import os
import re
import zipfile
from asyncio import Task
from datetime import datetime, timedelta
from typing import Any, Optional, Iterable, AsyncGenerator

import aiohttp
import aiohttp.web
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

_SECOND_CHANCE_PRIORITY = 1000000  # once-failed requests continue with a lower priority
_token = os.environ['INVEST_TOKEN']
_process_executor = concurrent.futures.ProcessPoolExecutor()  # for CPU-bound tasks
_session: Optional[aiohttp.ClientSession] = None

# History limit policy, updated once in a run.
_history_limit_period: timedelta = timedelta(minutes=1)
_history_limit_count: int = 10
_history_limit_watcher_task: Optional[Task] = None
_history_limit_policy_updated: bool = False

# Current history limits, updated after each HTTP response or by a timeout.
_history_limit = 1
_history_limit_timeout: datetime = datetime.now()
_history_limit_updated = asyncio.Event()
_history_request_queue: asyncio.PriorityQueue[tuple[int, asyncio.Event]] = asyncio.PriorityQueue()
_history_request_priority = 0  # earlier calls have highter priority


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


async def get_history_csvs(figi: str, first_year: int) -> AsyncGenerator[bytearray]:
    """ Download CSV candle history.

    :param figi: Instrument to download
    :param first_year: First year to download
    :return: A generator of bytearrays of the downloaded CSVs
    """

    # The main problem is to throttle requests by x-ratelimit-limit
    # Each instance of a function call adds its can_proceed event to the _history_request_queue priority queue.
    # Earlier calls have higher priority, accordingly to _history_request_priority.
    # _history_limit_loop() keeps tack of the request limit and fires the events accordingly.
    # Each HTTP response puts the next year request in the queue and updates the request limit info.
    # A 404 Not Found response means end of history. Any other error gives a second chance.
    # A failed second chance is logged as a warning and considered end of history.

    global _history_request_priority
    global _history_limit
    global _history_limit_count
    global _history_limit_period
    global _history_limit_policy_updated
    global _history_limit_timeout
    global _history_limit_watcher_task
    global _session

    year = first_year
    can_proceed = asyncio.Event()
    priority = _history_request_priority
    _history_request_priority += 1  # continuous numeration of all requests for the current program run
    first_chance_failed = False
    loop = asyncio.get_event_loop()
    if not _history_limit_watcher_task:
        _history_limit_watcher_task = asyncio.create_task(_history_limit_watcher())
    if not _session:  # should be initialized in an async context
        _session =  aiohttp.ClientSession(headers={'Authorization': 'Bearer ' + _token})

    with codetiming.Timer(initial_text=f"Downloading history of {figi}, starting with {first_year}...",
                          text=lambda elapsed: f"Downloaded {year-first_year} years of {figi} in {elapsed:.2f}s.",
                          logger=logger.debug):
        while year <= datetime.now().year:
            can_proceed.clear()
            _history_request_queue.put_nowait((priority, can_proceed))
            await can_proceed.wait()

            logger.debug(f"Requesting {figi} {year}.")
            async with _session.get(f'https://invest-public-api.tinkoff.ru/history-data?figi={figi}&year={year}') as response:
                # History request limits, updated once
                if not _history_limit_policy_updated and 'x-ratelimit-limit' in response.headers:
                    match = re.fullmatch(r'(?P<count1>[0-9]+).+?(?P<count2>[0-9]+).+?w=(?P<period>[0-9]+)',
                                         response.headers['x-ratelimit-limit'])
                    _history_limit_count = min(int(match.group('count1')), int(match.group('count2')))
                    _history_limit_period = timedelta(seconds=int(match.group('period')))
                    _history_limit_policy_updated = True

                # Remaining request limit timeout
                if 'x-ratelimit-reset' in response.headers:
                    limit_timeout_seconds = int(response.headers['x-ratelimit-reset'])
                    limit_timeout = datetime.now() + timedelta(seconds=limit_timeout_seconds)
                    if limit_timeout < _history_limit_timeout:
                        _history_limit_timeout = limit_timeout
                        _history_limit_updated.set()

                # Remaining requests
                if _history_limit == 0 and 'x-ratelimit-remaining' in response.headers:
                    _history_limit = int(response.headers['x-ratelimit-remaining'])
                    _history_limit_updated.set()

                # OK
                if response.ok:
                    if first_chance_failed:
                        first_chance_failed = False
                        priority -= _SECOND_CHANCE_PRIORITY
                    # Unzip in a parallel process.
                    zip_data = await response.content.read()
                    logger.debug(f"Received   {figi} {year}.")
                    yield await loop.run_in_executor(_process_executor, _extract, zip_data)
                    if year == datetime.now().year:
                        break
                    year += 1
                    continue

                # End of history
                if response.status == aiohttp.web.HTTPNotFound.status_code:
                    break

                message = response.headers.get('message', f"{response.reason}, no message")

                # Second chance failed, exit.
                if first_chance_failed:
                    logger.logger.error(f"{figi} {year}: {message}")
                    break

                # First chance failed, retry with a lower priority
                logger.logger.warning(f"{figi} {year}: {message}")
                first_chance_failed = True
                priority += _SECOND_CHANCE_PRIORITY  # retry at the end

        # The last one to leave, turn off the lights.
        if not _history_request_queue:
            _history_limit_watcher_task.cancel()
            _history_limit_watcher_task = None


def _extract(zip_data: bytes) -> bytearray:
    """ Unzip worker, called in a parallel process. """
    result = bytearray()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
        for csv_name in zip_file.namelist():
            result.extend(zip_file.read(csv_name))
    return result


async def _history_limit_watcher() -> None:
    """ Manage history request limit in an infinite parallel loop. """
    global _history_limit
    global _history_limit_timeout
    _history_limit_timeout = datetime.now() + _history_limit_period

    while True:  # will be cancelled from the outside (asyncio.CancelledError)
        while _history_limit > 0:
            _, event = await _history_request_queue.get()
            event.set()
            _history_limit -= 1

        wait_period = _history_limit_timeout - datetime.now()
        if wait_period > timedelta(0):
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(_history_limit_updated.wait(), wait_period.total_seconds())
        _history_limit_updated.clear()
        while _history_limit_timeout <= datetime.now():
            _history_limit = _history_limit_count
            _history_limit_timeout += _history_limit_period


def _close():
    """ Clean-up at exit. """
    if _history_limit_watcher_task:
        _history_limit_watcher_task.cancel()
    asyncio.run(_session.close())
    _process_executor.shutdown(wait=False, cancel_futures=True)
    logger.debug("Tinkoff API shut down.")

atexit.register(_close)
