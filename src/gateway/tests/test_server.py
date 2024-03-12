# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
from pyspark.sql.functions import col, substring
from pyspark.testing import assertDataFrameEqual


# pylint: disable=missing-function-docstring
class TestDataFrameAPI:
    """Tests of the dataframe side of SparkConnect."""

    # pylint: disable=singleton-comparison
    def test_filter(self, users_dataframe):
        outcome = users_dataframe.filter(col('paid_for_service') == True).collect()
        assert len(outcome) == 29

    def test_count(self, users_dataframe):
        outcome = users_dataframe.count()
        assert outcome == 100

    def test_with_column(self, users_dataframe, spark_session):
        expected = spark_session.createDataFrame(
            data=[('849118289', 'Brooke Jones', False)],
            schema=['user_id', 'name', 'paid_for_service'])
        outcome = users_dataframe.withColumn('user_id', substring(col('user_id'), 5, 9)).limit(
            1).collect()
        assertDataFrameEqual(outcome, expected)

    def test_cast(self, users_dataframe, spark_session):
        expected = spark_session.createDataFrame(
            data=[(849, 'Brooke Jones', False)],
            schema=['user_id', 'name', 'paid_for_service'])
        outcome = users_dataframe.withColumn(
            'user_id',
            substring(col('user_id'), 5, 3).cast('integer')).limit(
            1).collect()
        assertDataFrameEqual(outcome, expected)
