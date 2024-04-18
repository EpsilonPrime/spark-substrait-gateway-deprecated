# SPDX-License-Identifier: Apache-2.0
"""TPC-H Dataframe tests for the Spark to Substrait Gateway server."""
import pyspark
from pyspark import Row
from pyspark.sql.functions import avg, col, count, desc, try_sum
from pyspark.testing import assertDataFrameEqual


class TestTpchWithDataFrameAPI:
    """Runs the TPC-H standard test suite against the dataframe side of SparkConnect."""

    # pylint: disable=singleton-comparison
    def test_query_01(self, spark_session_with_tpch_dataset):
        expected = [
            Row(l_returnflag='A', l_linestatus='F', sum_qty=37734107.00,
                sum_base_price=56586554400.73, sum_disc_price=53758257134.87,
                sum_charge=55909065222.83, avg_qty=25.52,
                avg_price=38273.13, avg_disc=0.05, count_order=1478493),
        ]

        lineitem = spark_session_with_tpch_dataset.table('lineitem')
        outcome = lineitem.filter(col('l_shipdate') <= "1998-09-02").groupBy('l_returnflag',
                                                                             'l_linestatus').agg(
            try_sum('l_quantity').alias('sum_qty'),
            try_sum('l_extendedprice').alias('sum_base_price'),
            try_sum(col('l_extendedprice') * (1 - col('l_discount'))).alias('sum_disc_price'),
            try_sum(col('l_extendedprice') * (1 - col('l_discount')) * (1 + col('l_tax'))).alias(
                'sum_charge'),
            avg('l_quantity').alias('avg_qty'),
            avg('l_extendedprice').alias('avg_price'),
            avg('l_discount').alias('avg_disc'),
            count('*').alias('count_order'))

        sorted_outcome = outcome.sort('l_returnflag', 'l_linestatus').limit(1).collect()
        assertDataFrameEqual(sorted_outcome, expected, rtol=1e-2)

    def test_query_02(self, spark_session_with_tpch_dataset):
        expected = [
            Row(s_acctbal=9938.53, s_name='Supplier#000005359', n_name='UNITED KINGDOM',
                p_partkey=185358, p_mfgr='Manufacturer#4', s_address='QKuHYh,vZGiwu2FWEJoLDx04',
                s_phone='33-429-790-6131',
                s_comment='uriously regular requests hag'),
            Row(s_acctbal=9937.84, s_name='Supplier#000005969', n_name='ROMANIA',
                p_partkey=108438, p_mfgr='Manufacturer#1',
                s_address='ANDENSOSmk,miq23Xfb5RWt6dvUcvt6Qa', s_phone='29-520-692-3537',
                s_comment='efully express instructions. regular requests against the slyly fin'),
        ]

        part = spark_session_with_tpch_dataset.table('part')
        supplier = spark_session_with_tpch_dataset.table('supplier')
        partsupp = spark_session_with_tpch_dataset.table('partsupp')
        nation = spark_session_with_tpch_dataset.table('nation')
        region = spark_session_with_tpch_dataset.table('region')

        europe = region.filter(col('r_name') == 'EUROPE').join(
            nation, col('r_regionkey') == col('n_regionkey')).join(
            supplier, col('n_nationkey') == col('s_nationkey')).join(
            partsupp, col('s_suppkey') == col('ps_suppkey'))

        brass = part.filter((col('p_size') == 15) & (col('p_type').endswith('BRASS'))).join(
            europe, col('ps_partkey') == col('p_partkey'))

        minCost = brass.groupBy(col('ps_partkey')).agg(
            pyspark.sql.functions.min('ps_supplycost').alias('min'))

        outcome = brass.join(minCost, brass.ps_partkey == minCost.ps_partkey).filter(
            col('ps_supplycost') == col('min')).select('s_acctbal', 's_name', 'n_name', 'p_partkey',
                                                       'p_mfgr', 's_address', 's_phone',
                                                       's_comment')

        sorted_outcome = outcome.sort(
            desc('s_acctbal'), 'n_name', 's_name', 'p_partkey').limit(2).collect()
        assertDataFrameEqual(sorted_outcome, expected, rtol=1e-2)
