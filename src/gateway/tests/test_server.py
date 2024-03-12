# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
from pyspark.sql.functions import col


# pylint: disable=missing-function-docstring,too-few-public-methods
class TestDataFrameAPI:
    """Tests of the dataframe side of SparkConnect."""
    # pylint: disable=singleton-comparison
    def test_filter(self, users_dataframe):
        users_dataframe.filter(col('paid_for_service') == True)
