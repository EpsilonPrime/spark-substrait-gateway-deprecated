# SPDX-License-Identifier: Apache-2.0
"""A PySpark client that can send a single query to the gateway."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, array_contains, sum as sum_func, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


SCHEMA_DETECTION = False

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

if SCHEMA_DETECTION:
    schema_artists = df_artists = spark.read.parquet("/Users/davids/Desktop/artists.parquet").schema
else:
    schema_artists = StructType([
        StructField("mbid", StringType(), False),
        StructField("artist_mb", StringType(), True),
        StructField("artist_lastfm", StringType(), True),
        StructField("country_mb", StringType(), True),
        StructField("country_lastfm", StringType(), True),
        StructField("tags_mb", StringType(), True),
        StructField("tags_lastfm", StringType(), True),
        StructField("listeners_lastfm", IntegerType(), True),
        StructField("scrobbles_lastfm", IntegerType(), True),
        StructField("ambiguous_artist", BooleanType(), True),
    ])

df_artists = spark.read.format("parquet") \
    .schema(schema_artists) \
    .parquet("/Users/davids/Desktop/artists.parquet")

# pylint: disable=singleton-comparison
df_artists2 = \
    df_artists.withColumn("tags_lastfm", split(col("tags_lastfm"), "; ")) \
        .withColumn("listeners_lastfm", col("listeners_lastfm") \
                    .cast(IntegerType())) \
        .withColumn("ambiguous_artist", col("ambiguous_artist") \
                    .cast(BooleanType())) \
        .filter(col("ambiguous_artist") == False) \
        .filter(array_contains(col("tags_lastfm"), "pop")) \
        .groupBy("artist_lastfm") \
        .agg(sum_func("listeners_lastfm").alias("# of Listeners")) \
        .sort(desc("# of Listeners")) \
        .limit(10)

df_artists2.show()
