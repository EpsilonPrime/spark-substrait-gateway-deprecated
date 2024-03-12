# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
import pytest
from pyspark.sql.functions import col


class TestStuff:
    # pylint: disable=singleton-comparison
    def test_filter(self, spark_session, users_dataframe):
        users_dataframe.filter(col('paid_for_service') == True)
