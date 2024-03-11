# SPDX-License-Identifier: Apache-2.0
"""A PySpark client that can send sample queries to the gateway."""
import atexit
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, substring
from pyspark.sql.pandas.types import from_arrow_schema

from gateway.demo.mystream_database import create_mystream_database, delete_mystream_database
from gateway.demo.mystream_database import get_mystream_schema


# pylint: disable=fixme
def execute_query() -> None:
    """Runs a single sample query against the gateway."""
    # TODO -- Make this configurable.
    spark = SparkSession.builder.remote('sc://localhost:50051').getOrCreate()

    users_location = str(Path('users.parquet').absolute())
    schema_users = get_mystream_schema('users')

    df_users = spark.read.format('parquet') \
        .schema(from_arrow_schema(schema_users)) \
        .parquet(users_location)

    # pylint: disable=singleton-comparison
    #dataFrame.select(col("a"), substring_index(col("a"), ",", 1). as ("b"))
    df_users2 = df_users \
        .filter(col('paid_for_service') == True) \
        .withColumn('user_id', substring(col('user_id'), 4, 9)) \
        .sort(desc('name')) \
        .limit(10)

    df_users2.show()


if __name__ == '__main__':
    atexit.register(delete_mystream_database)
    path = create_mystream_database()
    execute_query()
