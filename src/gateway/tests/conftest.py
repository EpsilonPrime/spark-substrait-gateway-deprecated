# SPDX-License-Identifier: Apache-2.0
"""Test fixtures for pytest of the gateway server."""
import re
from pathlib import Path

import pytest
from gateway.backends.backend import Backend
from gateway.demo.mystream_database import (
    create_mystream_database,
    delete_mystream_database,
    get_mystream_schema,
)
from gateway.server import serve
from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql.session import SparkSession


def pytest_collection_modifyitems(items):
    for item in items:
        if 'source' in getattr(item, 'fixturenames', ()):
            source = re.search(r'\[([^,]+?)(-\d+)?]$', item.name).group(1)
            item.add_marker(source)
            continue
        item.add_marker('general')


# ruff: noqa: T201
def _create_local_spark_session() -> SparkSession:
    """Creates a local spark session for testing."""
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .config("spark.driver.memory", "2g")
        .appName('gateway')
        .getOrCreate()
    )

    conf = spark.sparkContext.getConf()
    # Dump the configuration settings for debug purposes.
    print("==== BEGIN SPARK CONFIG ====")
    for k, v in sorted(conf.getAll()):
        print(f"{k} = {v}")
    print("===== END SPARK CONFIG =====")

    yield spark
    spark.stop()


def _create_gateway_session(backend: str) -> SparkSession:
    """Creates a local gateway session for testing."""
    spark_gateway = (
        SparkSession
        .builder
        .remote('sc://localhost:50052')
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark-substrait-gateway.backend", backend)
        .appName('gateway')
        .create()
    )
    yield spark_gateway
    spark_gateway.stop()


@pytest.fixture(scope='function', autouse=True)
def manage_database() -> None:
    """Creates the mystream database for use throughout all the tests."""
    create_mystream_database()
    yield
    delete_mystream_database()


@pytest.fixture(scope='function', autouse=True)
def gateway_server():
    """Starts up a spark to substrait gateway service."""
    server = serve(50052, wait=False)
    yield
    server.stop(None)


@pytest.fixture(scope='function')
def users_location() -> str:
    """Provides the location of the users database."""
    return str(Path('users.parquet').resolve())


@pytest.fixture(scope='function')
def schema_users():
    """Provides the schema of the users database."""
    return get_mystream_schema('users')


@pytest.fixture(scope='session',
                params=['spark',
                        'gateway-over-duckdb',
                        'gateway-over-datafusion',
                        ])
def source(request) -> str:
    """Provides the source (backend) to be used."""
    return request.param


@pytest.fixture(scope='function')
def spark_session(source):
    """Provides spark sessions connecting to various backends."""
    match source:
        case 'spark':
            session_generator = _create_local_spark_session()
        case 'gateway-over-arrow':
            session_generator = _create_gateway_session('arrow')
        case 'gateway-over-datafusion':
            session_generator = _create_gateway_session('datafusion')
        case 'gateway-over-duckdb':
            session_generator = _create_gateway_session('duckdb')
        case _:
            raise NotImplementedError(f'No such session implemented: {source}')
    yield from session_generator


# pylint: disable=redefined-outer-name
@pytest.fixture(scope='function')
def users_dataframe(spark_session, schema_users, users_location):
    """Provides a ready to go dataframe over the users database."""
    return spark_session.read.format('parquet') \
        .schema(from_arrow_schema(schema_users)) \
        .parquet(users_location)


def find_tpch() -> Path:
    """Find the location of the TPC-H dataset."""
    current_location = Path('.').resolve()
    while current_location != Path('/'):
        location = current_location / 'third_party' / 'tpch' / 'parquet'
        if location.exists():
            return location.resolve()
        current_location = current_location.parent
    raise ValueError('TPC-H dataset not found')

def _register_table(spark_session: SparkSession, source: str, name: str) -> None:
    location = find_tpch() / name
    match source:
        case 'spark':
            spark_session.sql(
                f'CREATE OR REPLACE TEMPORARY VIEW {name} USING org.apache.spark.sql.parquet '
                f'OPTIONS ( path "{location}" )')
        case 'gateway-over-duckdb':
            # TODO -- Use a version of expand_location not in Backend.
            files = Backend.expand_location(location)
            if not files:
                raise ValueError(f"No parquet files found at {location}")
            files_str = ', '.join([f"'{f}'" for f in files])
            files_sql = f"CREATE OR REPLACE TABLE {name} AS FROM read_parquet([{files_str}])"
            spark_session.sql(files_sql)


@pytest.fixture(scope='function')
def spark_session_with_tpch_dataset(spark_session: SparkSession, source: str) -> SparkSession:
    """Add the TPC-H dataset to the current spark session."""
    _register_table(spark_session, source, 'customer')
    _register_table(spark_session, source, 'lineitem')
    _register_table(spark_session, source, 'nation')
    _register_table(spark_session, source, 'orders')
    _register_table(spark_session, source, 'part')
    _register_table(spark_session, source, 'partsupp')
    _register_table(spark_session, source, 'region')
    _register_table(spark_session, source, 'supplier')
    return spark_session
