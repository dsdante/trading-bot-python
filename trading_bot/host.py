from __future__ import annotations

import asyncio
from datetime import datetime
from types import TracebackType
from typing import Optional, Iterable, Type

import codetiming
import tinkoff.invest as ti

import db
import logger
import tinkoff_api as tapi


class Host:
    _db = db.DB()


    async def __aenter__(self) -> Host:
        await self.start()
        return self


    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        await self.stop()


    async def start(self) -> None:
        await self._db.connect()


    async def stop(self):
        await self._db.disconnect()


    async def deploy(self) -> None:
        """ Deploy the the trading. """
        await self._db.create(tapi.asset_types)


    async def update_instruments_async(self, asset_types: Optional[Iterable[str]] = None) -> None:
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
                await self._db.add_instruments(asset_type, db_instruments)


    def update_instruments(self, asset_types: Optional[Iterable[str]] = None) -> None:
        """ Update the instrument info. """
        asyncio.run(self.update_instruments_async(asset_types))


    async def download_history(self, figis: Optional[Iterable[str]] = None) -> None:
        """ Download candle history.

        :param figis: The list of instruments to download; None to download all known.
        """

        async def get_history_task(instrument, first_year):
            uid_binary = str(instrument.uid).encode()
            id_binary = str(instrument.id).encode()
            db_tasks = []
            async for csv in tapi.get_history_csvs(instrument.figi, first_year):
                csv = csv.replace(uid_binary, id_binary)
                csv = csv.replace(b';\n', b'\n')  # remove the trailing semicolon
                db_tasks.append(asyncio.create_task(self._db.save_candle_history(csv)))
            await asyncio.gather(*db_tasks)

        tasks = []
        async for instr, history_end in self._db.get_history_endings(figis):
            tasks.append(asyncio.create_task(get_history_task(instr, history_end.year)))
        await asyncio.gather(*tasks)
