# SPDX-License-Identifier: Apache-2.0
"""Test fixtures for pytest of the gateway server."""
from pathlib import Path

from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql.session import SparkSession
import pytest

from gateway.demo.mystream_database import create_mystream_database, delete_mystream_database
from gateway.demo.mystream_database import get_mystream_schema
from gateway.server import serve


def _create_local_spark_session() -> SparkSession:
    """Creates a local spark session for testing."""
    spark = (
        SparkSession
        .builder
        .master('local[*]')
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


@pytest.fixture(scope='session', autouse=True)
def manage_database() -> None:
    """Creates the mystream database for use throughout all the tests."""
    create_mystream_database()
    yield
    delete_mystream_database()


@pytest.fixture(scope='session', autouse=True)
def gateway_server():
    """Starts up a spark to substrait gateway service."""
    server = serve(50052, wait=False)
    yield
    server.stop(None)


@pytest.fixture(scope='session')
def users_location() -> str:
    """Provides the location of the users database."""
    return str(Path('users.parquet').resolve())


@pytest.fixture(scope='session')
def schema_users():
    """Provides the schema of the users database."""
    return get_mystream_schema('users')


@pytest.fixture(scope='session',
                params=['spark',
                        pytest.param('gateway-over-duckdb', marks=pytest.mark.xfail),
                        pytest.param('gateway-over-datafusion',
                                     marks=pytest.mark.xfail(
                                         reason='Datafusion Substrait missing in CI'))])
def source(request) -> str:
    """Provides the source (backend) to be used."""
    return request.param


@pytest.fixture(scope='session')
def spark_session(source):
    """Provides spark sessions connecting to various backends."""
    match source:
        case 'spark':
            session_generator = _create_local_spark_session()
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
