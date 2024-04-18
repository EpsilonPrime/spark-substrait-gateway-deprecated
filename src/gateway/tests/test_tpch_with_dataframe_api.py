# SPDX-License-Identifier: Apache-2.0
"""TPC-H Dataframe tests for the Spark to Substrait Gateway server."""
import datetime

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
        outcome = lineitem.filter(col('l_shipdate') <= '1998-09-02').groupBy('l_returnflag',
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

    def test_query_03(self, spark_session_with_tpch_dataset):
        expected = [
            Row(l_orderkey=2456423, revenue=406181.01, o_orderdate=datetime.date(1995, 3, 5),
                o_shippriority=0),
            Row(l_orderkey=3459808, revenue=405838.70, o_orderdate=datetime.date(1995, 3, 4),
                o_shippriority=0),
            Row(l_orderkey=492164, revenue=390324.06, o_orderdate=datetime.date(1995, 2, 19),
                o_shippriority=0),
            Row(l_orderkey=1188320, revenue=384537.94, o_orderdate=datetime.date(1995, 3, 9),
                o_shippriority=0),
            Row(l_orderkey=2435712, revenue=378673.06, o_orderdate=datetime.date(1995, 2, 26),
                o_shippriority=0),
        ]

        customer = spark_session_with_tpch_dataset.table('customer')
        lineitem = spark_session_with_tpch_dataset.table('lineitem')
        orders = spark_session_with_tpch_dataset.table('orders')

        fcust = customer.filter(col('c_mktsegment') == 'BUILDING')
        forders = orders.filter(col('o_orderdate') < '1995-03-15')
        flineitems = lineitem.filter(lineitem.l_shipdate > '1995-03-15')

        outcome = fcust.join(forders, col('c_custkey') == forders.o_custkey).select(
            'o_orderkey', 'o_orderdate', 'o_shippriority').join(
            flineitems, col('o_orderkey') == flineitems.l_orderkey).select(
            'l_orderkey',
            (col('l_extendedprice') * (1 - col('l_discount'))).alias('volume'),
            'o_orderdate',
            'o_shippriority').groupBy('l_orderkey', 'o_orderdate', 'o_shippriority').agg(
            try_sum('volume').alias('revenue')).select(
            'l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority')

        sorted_outcome = outcome.sort(desc('revenue'), 'o_orderdate').limit(5).collect()
        assertDataFrameEqual(sorted_outcome, expected, rtol=1e-2)

    def test_query_04(self, spark_session_with_tpch_dataset):
        expected = [
            Row(o_orderpriority='1-URGENT', order_count=10594),
            Row(o_orderpriority='2-HIGH', order_count=10476),
            Row(o_orderpriority='3-MEDIUM', order_count=10410),
            Row(o_orderpriority='4-NOT SPECIFIED', order_count=10556),
            Row(o_orderpriority='5-LOW', order_count=10487),
        ]

        orders = spark_session_with_tpch_dataset.table('orders')
        lineitem = spark_session_with_tpch_dataset.table('lineitem')

        forders = orders.filter(
            (col('o_orderdate') >= '1993-07-01') & (col('o_orderdate') < '1993-10-01'))
        flineitems = lineitem.filter(col('l_commitdate') < col('l_receiptdate')).select(
            'l_orderkey').distinct()

        outcome = flineitems.join(
            forders,
            col('l_orderkey') == col('o_orderkey')).groupBy('o_orderpriority').agg(
            count('o_orderpriority').alias('order_count'))

        sorted_outcome = outcome.sort('o_orderpriority').collect()
        assertDataFrameEqual(sorted_outcome, expected, rtol=1e-2)

    def test_query_05(self, spark_session_with_tpch_dataset):
        expected = [
            Row(n_name='INDONESIA', revenue=55502041.17),
            Row(n_name='VIETNAM', revenue=55295087.00),
            Row(n_name='CHINA', revenue=53724494.26),
            Row(n_name='INDIA', revenue=52035512.00),
            Row(n_name='JAPAN', revenue=45410175.70),
        ]

        customer = spark_session_with_tpch_dataset.table('customer')
        orders = spark_session_with_tpch_dataset.table('orders')
        lineitem = spark_session_with_tpch_dataset.table('lineitem')
        nation = spark_session_with_tpch_dataset.table('nation')
        region = spark_session_with_tpch_dataset.table('region')
        supplier = spark_session_with_tpch_dataset.table('supplier')

        forders = orders.filter(col('o_orderdate') >= '1994-01-01').filter(
            col('o_orderdate') < '1995-01-01')

        outcome = region.filter(col('r_name') == 'ASIA').join(  # r_name = 'ASIA'
            nation, col('r_regionkey') == col('n_regionkey')).join(
            supplier, col('n_nationkey') == col('s_nationkey')).join(
            lineitem, col('s_suppkey') == col('l_suppkey')).select(
            'n_name', 'l_extendedprice', 'l_discount', 'l_quantity', 'l_orderkey',
            's_nationkey').join(forders, col('l_orderkey') == forders.o_orderkey).join(
            customer, (col('o_custkey') == col('c_custkey')) & (
                    col('s_nationkey') == col('c_nationkey'))).select(
            'n_name',
            (col('l_extendedprice') * (1 - col('l_discount'))).alias('volume')).groupBy(
            'n_name').agg(try_sum('volume').alias('revenue'))

        sorted_outcome = outcome.sort('revenue').collect()

        assertDataFrameEqual(sorted_outcome, expected, rtol=1e-2)

    def test_query_06(self, spark_session_with_tpch_dataset):
        expected = [
            Row(revenue=123141078.23),
        ]

        lineitem = spark_session_with_tpch_dataset.table('lineitem')

        outcome = lineitem.filter((col('l_shipdate') >= '1994-01-01') &
                                  (col('l_shipdate') < '1995-01-01') &
                                  (col('l_discount') >= 0.05) &
                                  (col('l_discount') <= 0.07) &
                                  (col('l_quantity') < 24)).agg(
            try_sum(col('l_extendedprice') * col('l_discount'))).alias('revenue')

        assertDataFrameEqual(outcome, expected, rtol=1e-2)

    def test_query_07(self, spark_session_with_tpch_dataset):
        expected = [
            Row(supp_nation='FRANCE', cust_nation='GERMANY', l_year='1995', revenue=54639732.73),
            Row(supp_nation='FRANCE', cust_nation='GERMANY', l_year='1996', revenue=54633083.31),
            Row(supp_nation='GERMANY', cust_nation='FRANCE', l_year='1995', revenue=52531746.67),
            Row(supp_nation='GERMANY', cust_nation='FRANCE', l_year='1996', revenue=52520549.02),
        ]

        customer = spark_session_with_tpch_dataset.table('customer')
        orders = spark_session_with_tpch_dataset.table('orders')
        lineitem = spark_session_with_tpch_dataset.table('lineitem')
        supplier = spark_session_with_tpch_dataset.table('supplier')
        nation = spark_session_with_tpch_dataset.table('nation')

        fnation = nation.filter((nation.n_name == "FRANCE") | (nation.n_name == "GERMANY"))
        fline = lineitem.filter(
            (col('l_shipdate') >= "1995-01-01") & (col('l_shipdate') <= "1996-12-31"))

        suppNation = fnation.join(supplier, col('n_nationkey') == col('s_nationkey')).join(
            fline, col('s_suppkey') == col('l_suppkey')).select(
            col('n_name').alias('supp_nation'), 'l_orderkey', 'l_extendedprice', 'l_discount',
            'l_shipdate')

        outcome = fnation.join(customer, col('n_nationkey') == col('c_nationkey')).join(
            orders, col('c_custkey') == col('o_custkey')).select(
            col('n_name').alias('cust_nation'), 'o_orderkey').join(
            suppNation, col('o_orderkey') == suppNation.l_orderkey).filter(
            (col('supp_nation') == 'FRANCE') & (col('cust_nation') == 'GERMANY') | (
                    col('supp_nation') == 'GERMANY') & (col('cust_nation') == 'FRANCE')).select(
            'supp_nation', 'cust_nation', col('l_shipdate').substr(0, 4).alias('l_year'),
            (col('l_extendedprice') * (1 - col('l_discount'))).alias('volume')).groupBy(
            'supp_nation', 'cust_nation', 'l_year').agg(
            try_sum('volume').alias('revenue'))

        sorted_outcome = outcome.sort('supp_nation', 'cust_nation', 'l_year').collect()
        assertDataFrameEqual(sorted_outcome, expected, rtol=1e-2)
