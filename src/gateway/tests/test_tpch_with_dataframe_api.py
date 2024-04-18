# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.sql.functions import col, substring, avg, count, try_sum
from pyspark.testing import assertDataFrameEqual


# pylint: disable=missing-function-docstring
# ruff: noqa: E712
class TestTpchWithDataFrameAPI:
    """Runs the TPC-H standard test suite against the dataframe side of SparkConnect."""

    # pylint: disable=singleton-comparison
    def test_query_01(self, spark_session_with_tpch_dataset):
        expected = [
            Row(l_returnflag='A', l_linestatus='F', sum_qty=37734107.0,
                   sum_base_price=56586554400.730194, sum_disc_price=None, sum_charge=None,
                   avg_qty=25.522005853257337, avg_price=38273.129734621805,
                   avg_disc=0.04998529583842897, count_order=1478493),
        ]

        lineitem = spark_session_with_tpch_dataset.table('lineitem')
        outcome = lineitem.filter(col('l_shipdate') <= "1998-09-02").groupBy('l_returnflag',
                                                                             'l_linestatus').agg(
            try_sum('l_quantity').alias('sum_qty'),
            try_sum('l_extendedprice').alias('sum_base_price'),
            try_sum('l_extendedprice' * (1 - col('l_discount'))).alias('sum_disc_price'),
            try_sum('l_extendedprice' * (1 - col('l_discount')) * (1 + col('l_tax'))).alias('sum_charge'),
            avg('l_quantity').alias('avg_qty'),
            avg('l_extendedprice').alias('avg_price'),
            avg('l_discount').alias('avg_disc'),
            count('*').alias('count_order'))

        sorted_outcome = outcome.sort('l_returnflag', 'l_linestatus').limit(1).collect()
        assertDataFrameEqual(sorted_outcome, expected)

