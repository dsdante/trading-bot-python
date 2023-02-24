import asyncio

import tinkoff.invest as ti

import db
import tinkoff_api as tapi


async def deploy_async() -> None:
    """ Deploy the the trading asynchronously. """
    await db.create(tapi.asset_types)


def deploy() -> None:
    """ Deploy the trading bot. """
    asyncio.run(deploy_async())


def _api_to_db_instrument(api_instrument: ti.schemas.Instrument) -> db.Instrument:
    # Convert a Tinkoff API instrument to an SQLAlchemy instrument.
    db_instrument = db.Instrument()
    vars(db_instrument).update(vars(api_instrument))
    db_instrument.first_1min_candle_date = db_instrument.first_1min_candle_date.replace(tzinfo=None)
    db_instrument.first_1day_candle_date = db_instrument.first_1day_candle_date.replace(tzinfo=None)
    return db_instrument


async def update_instruments_async() -> None:
    """ Update the instrument info asynchronously. """
    async for asset_type, response in tapi.get_instruments():
        db_instruments = [_api_to_db_instrument(api_instrument) for api_instrument in response.instruments]
        await db.add_instruments(asset_type, db_instruments)


def update_instruments() -> None:
    """ Update the instrument info. """
    asyncio.run(update_instruments_async())