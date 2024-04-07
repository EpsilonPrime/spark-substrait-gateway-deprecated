# SPDX-License-Identifier: Apache-2.0
"""A PySpark client that can send sample queries to the gateway."""
import atexit
from pathlib import Path

import pyarrow
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.pandas.types import from_arrow_schema

from gateway.demo.mystream_database import create_mystream_database, delete_mystream_database
from gateway.demo.mystream_database import get_mystream_schema


# pylint: disable=fixme
def execute_query(spark_session: SparkSession) -> None:
    """Runs a single sample query against the gateway."""
    # df_customer = spark_session.read.parquet('../../../third_party/tpch/parquet/customer',
    #                                          mergeSchema=False)

    schema_customer = pyarrow.schema([
        pyarrow.field('c_custkey', pyarrow.int64(), False),
        pyarrow.field('c_name', pyarrow.string(), False),
        pyarrow.field('c_address', pyarrow.string(), False),
        pyarrow.field('c_nationkey', pyarrow.int64(), False),
        pyarrow.field('c_phone', pyarrow.string(), False),
        pyarrow.field('c_acctbal', pyarrow.float64(), False),
        pyarrow.field('c_acctbal', pyarrow.string(), False),
        pyarrow.field('c_comment', pyarrow.string(), False),
    ])

    df_customer = spark_session.read.format('parquet') \
        .schema(from_arrow_schema(schema_customer)) \
        .parquet('../../../third_party/tpch/parquet/customer',
                 mergeSchema=False).createOrReplaceTempView('customer')

    print(df_customer.limit(10).show())


if __name__ == '__main__':
    atexit.register(delete_mystream_database)
    path = create_mystream_database()

    # TODO -- Make this configurable.
    spark = SparkSession.builder.remote('sc://localhost:50051').getOrCreate()
    execute_query(spark)
