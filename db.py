import collections
import getpass
import http
import io
import itertools
import os
import sys
import time
import zipfile
from datetime import datetime
from typing import Optional, List
from uuid import UUID

import psycopg2
import psycopg2.extensions
import requests as requests
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
import tinkoff.invest as tin
from sqlalchemy import ForeignKey, func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from sqlalchemy.schema import CreateColumn
from sqlalchemy.types import Text

token = os.environ['INVEST_TOKEN']

db_engine = sa.create_engine(sa.URL.create(
    drivername='postgresql+psycopg2',
    username=getpass.getuser(),
    database='trading_bot'),
    echo=False)

# { name: Client.getter_function }
instrument_types = {
    'bond': 'bonds',
    'currency': 'currencies',
    'etf': 'etfs',
    'future': 'futures',
    'option': 'options',
    'share': 'shares',
}


#region Database schema

# Generate IDENTITY columns instead of SERIAL
# https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#postgresql-10-identity-columns
@compiles(CreateColumn, 'postgresql')
def use_identity(element, compiler, **kw):
    text = compiler.visit_create_column(element, **kw)
    text = text.replace('SERIAL', 'INT GENERATED BY DEFAULT AS IDENTITY')
    return text


class Base(DeclarativeBase):
    pass


class InstrumentType(Base):
    __tablename__ = 'instrument_type'

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(type_=Text, unique=True)

    instruments: Mapped[List['Instrument']] = relationship(back_populates='type')

    def __repr__(self) -> str:
        return self.name


class Instrument(Base):
    __tablename__ = 'instrument'

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[UUID] = mapped_column(unique=True)
    figi: Mapped[Optional[str]] = mapped_column(type_=Text)
    name: Mapped[str] = mapped_column(type_=Text)
    type_id: Mapped[int] = mapped_column('type', ForeignKey('instrument_type.id'))
    lot: Mapped[int] = mapped_column()
    first_1min_candle_date: Mapped[datetime] = mapped_column()
    first_1day_candle_date: Mapped[datetime] = mapped_column()
    for_qual_investor_flag: Mapped[bool] = mapped_column()
    has_earliest_candles: Mapped[bool] = mapped_column(default=False)

    type: Mapped['InstrumentType'] = relationship(back_populates='instruments')
    candles: Mapped[List['Candle']] = relationship(back_populates='instrument')

    def __repr__(self) -> str:
        return f'{self.name} ({self.figi})'


class Candle(Base):
    __tablename__ = 'candle'

    # Must be compatible with the history CSV files
    instrument_id: Mapped[int] = mapped_column('instrument', ForeignKey('instrument.id'), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(primary_key=True)
    open: Mapped[float] = mapped_column()
    close: Mapped[float] = mapped_column()
    high: Mapped[float] = mapped_column()
    low: Mapped[float] = mapped_column()
    volume: Mapped[int] = mapped_column()

    instrument: Mapped['Instrument'] = relationship(back_populates='candles')

    def __repr__(self) -> str:
        return f'{self.instrument_id:05} {self.timestamp:%Y-%m-%d %H:%M} ({self.low}-{self.open}-{self.close}-{self.high})'

#endregion Database schema


#region Database maintenance

def deploy():
    """
    Create a database and fill it with static data

    If the database doesn't exist, the user must have the CREATEDB privilige.
    """

    print(f"Deploying database {db_engine.url.database}...", end="")
    connection = None
    # noinspection PyUnresolvedReferences
    try:
        # Try creating the database if it doesn't exist.
        # Avoid starting a transaction. https://stackoverflow.com/a/68112827/934618
        connection = psycopg2.connect('dbname=postgres')
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE {db_engine.url.database};')
    except psycopg2.errors.DuplicateDatabase:
        pass
    finally:
        if connection:
            connection.close()

    # Creating the schema
    Base.metadata.create_all(db_engine)

    # Static data
    instrument_type_data = [{'name': name} for name in instrument_types]
    with Session(db_engine) as db:
        db.execute(pg.insert(InstrumentType).on_conflict_do_nothing(), instrument_type_data)
        db.commit()

    print(f" done.\n")


def download_instrument_info():
    """ Download the list of instruments and their properties """

    print("Downloading the instruments...")
    loaded_count = 0
    updated_count = 0
    with Session(db_engine) as db, tin.Client(token) as client:
        select_query = sa.select(InstrumentType.name, InstrumentType.id)
        # noinspection PyTypeChecker
        # { instrument.name: instrument.id }
        instrument_ids = dict(db.execute(select_query).all())

        try:
            for instrument_type in instrument_types:
                print(instrument_types[instrument_type].title() + ": ", end="")
                sys.stdout.flush()
                # instrument_types.values() is a list of getter functions in Client
                instruments = getattr(client.instruments, instrument_types[instrument_type])().instruments
                loaded_count += 1
                # A missing column in the 'instrument' table
                type_column = { 'type_id': instrument_ids[instrument_type] }
                instrument_data = (instrument.__dict__ | type_column for instrument in instruments)

                count = db.query(Instrument.id).count()
                insert_query = pg.insert(Instrument).on_conflict_do_update(index_elements=[Instrument.uid], set_=Instrument.__table__.c)
                # noinspection PyTypeChecker
                db.execute(insert_query, instrument_data)
                db.commit()
                added = db.query(Instrument).count() - count
                print(f"{len(instruments)} loaded, {added} added")
                updated_count += added > 0
        except KeyboardInterrupt:
            print("Ctrl+C")
        finally:
            print(f"{loaded_count} instruments loaded, {updated_count} updated.\n")


def download_history():
    """ Download the candle history """

    print("Downloading candle history...")
    csv_columns = [
        Candle.instrument_id,
        Candle.timestamp,
        Candle.open,
        Candle.close,
        Candle.high,
        Candle.low,
        Candle.volume,
    ]
    csv_columns = [column.name for column in csv_columns]
    TimeSpan = collections.namedtuple('TimeSpan', 'earliest, latest')
    required_headers = {'x-ratelimit-remaining', 'x-ratelimit-reset'}

    with Session(db_engine) as db:
        dbapi = db.connection().connection.cursor()  # a low-level DB API for importing CSV
        instruments = db.query(Instrument).order_by(Instrument.type_id, Instrument.name).all()
        candle_spans = db.query(Candle.instrument_id, func.min(Candle.timestamp), func.max(Candle.timestamp))\
                         .group_by(Candle.instrument_id)\
                         .all()
        candle_spans = {instrument_id: TimeSpan(earliest.year, latest.year) for (instrument_id, earliest, latest) in candle_spans}
        updated_instruments = set()
        sleep_time = 0  # HTTP API request frequency limit

        try:
            for instrument in instruments:
                print(f"{str(instrument.type).title()} {instrument}: ", end="")
                sys.stdout.flush()

                # Determining potentially missing date ranges
                candle_span = candle_spans.get(instrument.id, None)
                if candle_span:
                    years = range(datetime.now().year, candle_span.latest, -1)
                    if instrument.has_earliest_candles:
                        if candle_span.latest == datetime.now().year:
                            print("up to date")
                            continue
                    else:
                        years = itertools.chain(years, range(candle_span.earliest - 1, 0, -1))
                else:
                    years = range(datetime.now().year, 0, -1)

                uid_binary = str(instrument.uid).encode()
                id_binary = str(instrument.id).encode()
                loaded_any = False

                for year in years:
                    if sleep_time > 0:
                        print(f"(waiting {sleep_time} s) ", end="")
                        sys.stdout.flush()
                        time.sleep(sleep_time)
                        sleep_time = 0

                    while True:
                        response = requests.get(f'https://invest-public-api.tinkoff.ru/history-data?figi={instrument.figi}&year={year}',
                                                headers={'Authorization': 'Bearer ' + token})
                        if required_headers.issubset(response.headers):
                            break
                        print("(invalid response, waiting 60 s) ")
                        sys.stdout.flush()
                        time.sleep(60)

                    if response.headers['x-ratelimit-remaining'] == '0':
                        sleep_time = int(response.headers['x-ratelimit-reset']) + 1

                    if response.status_code == http.HTTPStatus.NOT_FOUND:
                        if year == datetime.now().year:
                            # Some instruments may not have recorded this year.
                            if instrument.has_earliest_candles and candle_span.latest == datetime.now().year - 1:
                                print("up to date", end="")
                                break
                            print(f"(no {datetime.now().year}) ", end="")
                            sys.stdout.flush()
                            continue
                        else:
                            instrument.has_earliest_candles = True
                            db.commit()
                            print("(earliest date)", end="")
                            break
                    elif response.status_code != http.HTTPStatus.OK:
                        print(response.headers.get('message', f"{http.HTTPStatus(response.status_code).name}, no message"), end="")
                        break
                    print(f"{year} ", end="")
                    sys.stdout.flush()

                    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                    for csv_name in zip_file.namelist():
                        csv = zip_file.read(csv_name)
                        # replacing uid with id, removing semicolons at the end of lines
                        csv = csv.replace(uid_binary, id_binary).replace(b';\n', b'\n')
                        dbapi.copy_from(io.BytesIO(csv), Candle.__tablename__, sep=';', columns=csv_columns)
                        db.commit()
                        updated_instruments.add(instrument.id)
                print()

        except KeyboardInterrupt:
            print("Ctrl+C")
        except:
            print()
            raise
        finally:
            print(f"{len(updated_instruments)} instruments updated.\n")


def backup(path: str):
    # TODO
    pass


def restore(path: str):
    # TODO
    pass

#endregion Database maintenance
