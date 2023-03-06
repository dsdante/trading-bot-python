import asyncio
from datetime import datetime
from typing import Optional, Iterable

import codetiming
import tinkoff.invest as ti

import db
import logger
import tinkoff_api as tapi


async def deploy_async() -> None:
    """ Deploy the the trading asynchronously. """
    await db.create(tapi.asset_types)


def deploy() -> None:
    """ Deploy the trading bot. """
    with codetiming.Timer(text="Deployed the trading bot in {:.2f}s.", logger=logger.info):
        asyncio.run(deploy_async())


async def update_instruments_async(asset_types: Optional[Iterable[str]] = None) -> None:
    """ Update the instrument info asynchronously. """

    def api_to_db_instrument(api_instrument: ti.schemas.Instrument) -> db.Instrument:
        # Convert a Tinkoff API instrument to an SQLAlchemy instrument.
        def api_to_db_datetime(dt: datetime) -> Optional[datetime]:
            # Clear timezone info, return None instead of 1970.01.01
            return dt.replace(tzinfo=None) if dt.timestamp() else None
        db_instrument = db.Instrument()
        vars(db_instrument).update(vars(api_instrument))
        db_instrument.first_1min_candle_date = api_to_db_datetime(db_instrument.first_1min_candle_date)
        db_instrument.first_1day_candle_date = api_to_db_datetime(db_instrument.first_1day_candle_date)
        return db_instrument

    count = 0
    with codetiming.Timer(initial_text=f"Updating instruments...",
                          text=lambda elapsed: f"Updated {count} instruments in {elapsed:.2f}s.",
                          logger=logger.info):
        async for asset_type, response in tapi.get_instruments(asset_types):
            db_instruments = [api_to_db_instrument(api_instrument) for api_instrument in response.instruments]
            count += len(db_instruments)
            await db.add_instruments(asset_type, db_instruments)


def update_instruments(asset_types: Optional[Iterable[str]] = None) -> None:
    """ Update the instrument info. """
    asyncio.run(update_instruments_async(asset_types))


async def download_history_async(figis: Optional[Iterable[str]] = None) -> None:
    """ Download candle history asynchronously.

    :param figis: The list of instruments to download; None to download all known.
    """
    async for instrument, history_end in db.get_history_endings(figis):
        with open(f'{instrument.figi}.csv', mode='wb') as file:
            async for csv in tapi.get_history_csvs(instrument.figi, history_end.year):
                csv = csv.replace(str(instrument.uid).encode(), str(instrument.id).encode())  # replace UID with ID
                csv = csv.replace(b';\n', b'\n')  # remove the trailing semicolon
                file.write(csv)


def download_history(figis: Optional[Iterable[str]] = None) -> None:
    """ Download candle history.

    :param figis: The list of instruments to download; None to download all known.
    """
    asyncio.run(download_history_async(figis))
