from __future__ import annotations

import asyncio
import getpass
import uuid
from datetime import datetime
from types import TracebackType
from typing import Optional, Iterable, Sequence, AsyncGenerator, Type

import codetiming
import psycopg
import psycopg_pool
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
import sqlalchemy.exc
from psycopg.abc import Buffer
from sqlalchemy import ForeignKey
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine, AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from sqlalchemy.types import Text

import logger


# region Database schema

class Base(DeclarativeBase):
    """ An SQLAlchemy declarative base """
    pass


class AssetType(Base):
    """ Currency, share, bond, etc as a class """
    __tablename__ = 'asset_type'

    id: Mapped[int] = mapped_column(sa.Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(type_=Text, unique=True)

    instruments_ref: Mapped[list[Instrument]] = relationship(back_populates='asset_type_ref', lazy='raise')

    def __repr__(self) -> str:
        return self.name


class Instrument(Base):
    """ A single currency, share, bond, etc """
    __tablename__ = 'instrument'

    # Compatible with the API instrument response
    id: Mapped[int] = mapped_column(sa.Identity(), primary_key=True)
    uid: Mapped[uuid.UUID] = mapped_column(unique=True)
    figi: Mapped[Optional[str]] = mapped_column(type_=Text)
    name: Mapped[str] = mapped_column(type_=Text)
    asset_type_id: Mapped[int] = mapped_column('asset_type', ForeignKey('asset_type.id'))
    lot: Mapped[int] = mapped_column()  # minimum size of a deal
    otc_flag: Mapped[bool] = mapped_column()  # traded over the counter
    for_qual_investor_flag: Mapped[bool] = mapped_column()  # only available for qualified investors
    api_trade_available_flag: Mapped[bool] = mapped_column()
    first_1min_candle_date: Mapped[Optional[datetime]] = mapped_column()
    first_1day_candle_date: Mapped[Optional[datetime]] = mapped_column()

    asset_type_ref: Mapped[AssetType] = relationship(back_populates='instruments_ref', lazy='raise')
    candles_ref: Mapped[list[Candle]] = relationship(back_populates='instrument_ref', lazy='raise')

    def __repr__(self) -> str:
        return f'{self.name} ({self.figi})'


class Candle(Base):
    """ Historical pricing datum for an instrument """
    __tablename__ = 'candle'

    # Compatible with the history CSV files
    instrument_id: Mapped[int] = mapped_column('instrument', ForeignKey('instrument.id'), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(primary_key=True)
    open: Mapped[float] = mapped_column()
    close: Mapped[float] = mapped_column()
    high: Mapped[float] = mapped_column()
    low: Mapped[float] = mapped_column()
    volume: Mapped[int] = mapped_column()

    instrument_ref: Mapped[Instrument] = relationship(back_populates='candles_ref', lazy='raise')

    def __repr__(self) -> str:
        return f'{self.instrument_id:05} {self.timestamp:%Y-%m-%d %H:%M} ({self.low}-{self.open}-{self.close}-{self.high})'

# endregion Database schema


class DB:
    _engine = create_async_engine(sa.URL.create(
        drivername='postgresql+psycopg',
        username=getpass.getuser(),
        database='trading_bot'),
        echo=False)
    _start_session: async_sessionmaker[AsyncSession] = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    _asset_types_lock = asyncio.Lock()
    _asset_types: Optional[dict[str, AssetType]] = None
    _pg_pool: psycopg_pool.AsyncConnectionPool


    async def __aenter__(self) -> DB:
        await self.connect()
        return self


    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        await self.disconnect()


    async def connect(self) -> None:
        self._pg_pool = psycopg_pool.AsyncConnectionPool(f'dbname={self._engine.url.database} user={self._engine.url.username}')


    async def disconnect(self) -> None:
        # TODO: @contextlib.asynccontextmanager
        await self._pg_pool.close()
        await self._engine.dispose()
        logger.debug("Psycopg engine disposed.")


    async def create(self, asset_types: Iterable[str]) -> None:
        """ Create a database and fill it with static data.

        :param asset_types: Names of asset types
        """
        with codetiming.Timer(text=f"Database {self._engine.url.database} deployed in {{:.2f}}s.", logger=logger.debug):
            # Creating the schema
            try:
                async with self._engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)

            except sa.exc.OperationalError:
                # If the DB did not exist, create it first, then retry.
                async with await psycopg.AsyncConnection.connect('dbname=postgres', autocommit=True) as connection:
                    async with connection.cursor() as cursor:
                        await cursor.execute(f'CREATE DATABASE {self._engine.url.database};')
                # Second attempt
                async with self._engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)

            # Static data
            values = [{'name': name} for name in asset_types]
            async with self._start_session() as session:
                await session.execute(pg.insert(AssetType).on_conflict_do_nothing(), values)
                await session.commit()

        self._asset_types = None


    async def _get_asset_types(self) -> dict[str, AssetType]:
        # Update and return asset types by their IDs.
        async with self._asset_types_lock:
            if self._asset_types is not None:
                return self._asset_types
            with codetiming.Timer(text=lambda elapsed: f"Read {len(self._asset_types)} asset types in {elapsed:.2f}s.", logger=logger.debug):
                async with self._start_session() as session:
                    response = await session.execute(sa.select(AssetType))
                self._asset_types = {asset_type.name: asset_type.id for asset_type, in response.all()}
            return self._asset_types


    async def add_instruments(self, asset_type: str, instruments: Sequence[Instrument]) -> None:
        """ Add new instruments (of the same type) to the database.

        :param asset_type: The type of the instruments
        :param instruments: DB instrument objects
        """
        asset_types = await self._get_asset_types()
        asset_type_field = {Instrument.asset_type_id.key: asset_types[asset_type]}
        instrument_data = [vars(instrument) | asset_type_field for instrument in instruments]
        stmt = pg.insert(Instrument)
        updated_data = {column.name: column for column in stmt.excluded if not column.primary_key}
        stmt = stmt.on_conflict_do_update(index_elements=[Instrument.uid], set_=updated_data)
        with codetiming.Timer(text=lambda elapsed: f"Saved {len(instrument_data)} {asset_type} in {elapsed:.2f}s.", logger=logger.debug):
            async with self._start_session() as session:
                await session.execute(stmt, instrument_data)
                await session.commit()


    async def get_history_endings(self, figis: Optional[Iterable[str]] = None) -> AsyncGenerator[tuple[Instrument, datetime]]:
        """ Get the last candle timestamp for each instrument.

        :param figis: List of instrument FIGIs to request. None means all known instruments with a FIGI.
        """
        subquery = sa.select(Candle.instrument_id,
                             sa.func.max(Candle.timestamp).label('latest'))\
            .group_by(Candle.instrument_id)\
            .subquery()
        query = sa.select(Instrument,
                          sa.func.coalesce(subquery.c.latest, Instrument.first_1min_candle_date).label('history_end'))\
            .join(subquery, Instrument.id == subquery.c.instrument_id, isouter=True)\
            .where((Instrument.figi != None) & (Instrument.first_1min_candle_date != None))\
            .order_by('history_end')

        with codetiming.Timer(initial_text="Requesting history endings...",
                              text="Received history endings in {:.2f}s.",
                              logger=logger.debug):
            async with self._start_session() as session:
                response = await session.execute(query)

        for instrument, history_end in response:
            if figis is None or instrument.figi in figis:
                yield instrument, history_end

    async def save_candle_history(self, csv: Buffer) -> None:
        temp_table = "candle_" + str(uuid.uuid4().hex)[:56]
        with codetiming.Timer(text=f"Saved {len(csv) / 1024 / 1024:.2f} MB of candles in {{:.2f}} s"):
            async with self._pg_pool.connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(f'CREATE TEMP TABLE {temp_table} (LIKE candle) ON COMMIT DROP')
                    async with cursor.copy(f"COPY {temp_table}(instrument, timestamp, open, close, high, low, volume) FROM STDIN CSV DELIMITER ';'") as copy:
                        await copy.write(csv)
                    await cursor.execute(f'INSERT INTO candle SELECT * FROM {temp_table} ON CONFLICT DO NOTHING')
                await connection.commit()
