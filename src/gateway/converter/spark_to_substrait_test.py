# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait plan conversion routines."""
import os
from pathlib import Path

from google.protobuf import text_format
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.connect.proto import base_pb2 as spark_base_pb2
from pyspark.sql.pandas.types import from_arrow_schema
from substrait.gen.proto import plan_pb2

from gateway.converter.conversion_options import duck_db
from gateway.converter.spark_to_substrait import SparkSubstraitConverter
from gateway.converter.sql_to_substrait import SqlConverter
from gateway.demo.mystream_database import get_mystream_schema
from gateway.tests.conftest import users_location

test_case_directory = Path(os.path.dirname(os.path.realpath(__file__))) / 'data'

substrait_test_case_paths = [f for f in test_case_directory.iterdir() if f.name.endswith('.spark')]

substrait_test_case_names = [os.path.basename(p).removesuffix('.spark') for p in substrait_test_case_paths]


sql_test_case_paths = [f for f in test_case_directory.iterdir() if f.name.endswith('.sql')]

sql_test_case_names = [os.path.basename(p).removesuffix('.sql') for p in sql_test_case_paths]


# pylint: disable=E1101
@pytest.mark.parametrize(
    'path',
    substrait_test_case_paths,
    ids=substrait_test_case_names,
)
def test_plan_conversion(request, path):
    """Test the conversion of a Spark plan to a Substrait plan."""
    # Read the Spark plan to convert.
    with open(path, "rb") as file:
        plan_prototext = file.read()
    spark_plan = text_format.Parse(plan_prototext, spark_base_pb2.Plan())

    # The expected result is in the corresponding Substrait plan.
    with open(path.with_suffix('.splan'), "rb") as file:
        splan_prototext = file.read()
    substrait_plan = text_format.Parse(splan_prototext, plan_pb2.Plan())

    convert = SparkSubstraitConverter(duck_db())
    substrait = convert.convert_plan(spark_plan)

    if request.config.getoption('rebuild_goldens'):
        if substrait != substrait_plan:
            with open(path.with_suffix('.splan'), "wt", encoding='utf-8') as file:
                file.write(text_format.MessageToString(substrait))
        return

    assert substrait == substrait_plan


def _get_users_dataframe():
    spark_session = (
        SparkSession
        .builder
        .master('local')
        .config("spark.driver.bindAddress", "127.0.0.1")
        .appName('gateway')
        .getOrCreate()
    )
    users_dataframe = spark_session.read.format('parquet') \
        .schema(from_arrow_schema(get_mystream_schema('users'))) \
        .parquet(str(Path('users.parquet').absolute()))
    return users_dataframe


# pylint: disable=E1101
@pytest.mark.parametrize(
    'path',
    sql_test_case_paths,
    ids=sql_test_case_names,
)
def test_sql_conversion(request, path):
    """Test the conversion of SQL to a Substrait plan."""
    # Read the Spark plan to convert.
    with open(path, "rb") as file:
        sql_bytes = file.read()
    sql = sql_bytes.decode('utf-8')

    # The expected result is in the corresponding Substrait plan.
    with open(path.with_suffix('.sql-splan'), "rb") as file:
        splan_prototext = file.read()
    substrait_plan = text_format.Parse(splan_prototext, plan_pb2.Plan())

    users_dataframe = _get_users_dataframe()
    users_dataframe.createOrReplaceTempView('users')
    substrait = SqlConverter().convert_sql(str(sql))

    if request.config.getoption('rebuild_goldens'):
        if substrait != substrait_plan:
            with open(path.with_suffix('.sql-splan'), "wt", encoding='utf-8') as file:
                file.write(text_format.MessageToString(substrait))
        return

    assert substrait == substrait_plan
