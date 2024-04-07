# SPDX-License-Identifier: Apache-2.0
"""A PySpark client that can send sample queries to the gateway."""
from pathlib import Path

import pyarrow
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.pandas.types import from_arrow_schema


# pylint: disable=fixme
def future_get_customer_database(spark_session: SparkSession) -> DataFrame:
    # TODO -- Use this when server-side schema evaluation is available.
    location_customer = str(Path('../../third_party/tpch/parquet/customer').absolute())

    return spark_session.read.parquet(location_customer,
                                              mergeSchema=False)


def get_customer_database(spark_session: SparkSession) -> DataFrame:
    location_customer = str(Path('../../third_party/tpch/parquet/customer').absolute())

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

    return (spark_session.read.format('parquet')
            .schema(from_arrow_schema(schema_customer))
            .load(location_customer + '/*.parquet'))


# pylint: disable=fixme
def execute_query(spark_session: SparkSession) -> None:
    """Runs a single sample query against the gateway."""
    df_customer = get_customer_database(spark_session)

    print(df_customer.limit(10).show())


if __name__ == '__main__':
    # TODO -- Make this configurable.
    spark = SparkSession.builder.remote('sc://localhost:50051').getOrCreate()
    execute_query(spark)
