from __future__ import annotations

import asyncio
import logging
from datetime import datetime
import getpass
import uuid
from typing import Optional, Iterable, Sequence

import codetiming
import psycopg
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
import sqlalchemy.exc
from sqlalchemy import ForeignKey
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine, AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from sqlalchemy.types import Text

import logger

_engine = create_async_engine(sa.URL.create(
    drivername='postgresql+psycopg',
    username=getpass.getuser(),
    database='trading_bot'),
    echo=False)

_start_session: async_sessionmaker[AsyncSession] = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
_asset_types_lock = asyncio.Lock()
_asset_types: Optional[dict[str, AssetType]] = None


#region Database schema

class Base(DeclarativeBase):
    """ An SQLAlchemy declarative base """
    pass


class AssetType(Base):
    """ Currency, share, bond, etc as a class """
    __tablename__ = 'asset_type'

    id: Mapped[int] = mapped_column(sa.Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(type_=Text, unique=True)

    instruments_ref: Mapped[list[Instrument]] = relationship(back_populates='asset_type_ref')

    def __repr__(self) -> str:
        return self.name


class Instrument(Base):
    """ A single currency, share, bond, etc """
    __tablename__ = 'instrument'

    id: Mapped[int] = mapped_column(sa.Identity(), primary_key=True)
    uid: Mapped[uuid.UUID] = mapped_column(unique=True)
    figi: Mapped[Optional[str]] = mapped_column(type_=Text)
    name: Mapped[str] = mapped_column(type_=Text)
    asset_type_id: Mapped[int] = mapped_column('asset_type', ForeignKey('asset_type.id'))
    lot: Mapped[int] = mapped_column()
    first_1min_candle_date: Mapped[datetime] = mapped_column()
    first_1day_candle_date: Mapped[datetime] = mapped_column()
    for_qual_investor_flag: Mapped[bool] = mapped_column()
    has_earliest_candles: Mapped[bool] = mapped_column(default=False)

    asset_type_ref: Mapped[AssetType] = relationship(back_populates='instruments_ref')
    candles_ref: Mapped[list[Candle]] = relationship(back_populates='instrument_ref')

    def __repr__(self) -> str:
        return f'{self.name} ({self.figi})'


class Candle(Base):
    """ Historical pricing datum for an instrument """
    __tablename__ = 'candle'

    # Must be compatible with the history CSV files
    instrument_id: Mapped[int] = mapped_column('instrument', ForeignKey('instrument.id'), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(primary_key=True)
    open: Mapped[float] = mapped_column()
    close: Mapped[float] = mapped_column()
    high: Mapped[float] = mapped_column()
    low: Mapped[float] = mapped_column()
    volume: Mapped[int] = mapped_column()

    instrument_ref: Mapped[Instrument] = relationship(back_populates='candles_ref')

    def __repr__(self) -> str:
        return f'{self.instrument_id:05} {self.timestamp:%Y-%m-%d %H:%M} ({self.low}-{self.open}-{self.close}-{self.high})'

#endregion Database schema


async def create(asset_types: Iterable[str]) -> None:
    """ Create a database and fill it with static data.

    :param asset_types: Names of asset types
    """
    with codetiming.Timer(text=f"Database {_engine.url.database} deployed in {{:.2f}}s.", logger=logger.debug):
        # Creating the schema
        try:
            async with _engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)

        except sa.exc.OperationalError:
            # If the DB did not exist, create it first, then retry.
            async with await psycopg.AsyncConnection.connect('dbname=postgres', autocommit=True) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(f'CREATE DATABASE {_engine.url.database};')
            # Second attempt
            async with _engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)

        # Static data
        values = [{'name': name} for name in asset_types]
        async with _start_session() as session:
            await session.execute(pg.insert(AssetType).on_conflict_do_nothing(), values)
            await session.commit()

    global _asset_types
    _asset_types = None


async def _get_asset_types() -> dict[str, AssetType]:
    # Update and return asset types by their IDs.
    async with _asset_types_lock:
        global _asset_types
        if _asset_types is not None:
            return _asset_types
        with codetiming.Timer(text=lambda elapsed: f"Read {len(_asset_types)} asset types in {elapsed:.2f}s.", logger=logger.debug):
            async with _start_session() as session:
                response = await session.execute(sa.select(AssetType))
            _asset_types = {asset_type.name: asset_type.id for asset_type, in response.all()}
        return _asset_types


async def add_instruments(asset_type: str, instruments: Sequence[Instrument]) -> None:
    """ Add new instruments (of the same type) to the database.

    :param asset_type: The type of the instruments
    :param instruments: DB instrument objects
    """
    asset_types = await _get_asset_types()
    asset_type_field = {Instrument.asset_type_id.key: asset_types[asset_type]}
    instrument_data = [vars(instrument) | asset_type_field for instrument in instruments]
    stmt = pg.insert(Instrument)
    updated_data = {column.name: column for column in stmt.excluded if not column.primary_key}
    stmt = stmt.on_conflict_do_update(index_elements=[Instrument.uid], set_=updated_data)
    with codetiming.Timer(text=lambda elapsed: f"Saved {len(instrument_data)} {asset_type} in {elapsed:.2f}s.", logger=logger.debug):
        async with _start_session() as session:
            await session.execute(stmt, instrument_data)
            await session.commit()
