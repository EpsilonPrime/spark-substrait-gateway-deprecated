# SPDX-License-Identifier: Apache-2.0
"""Routines to provide information regarding the TPC-H parquet test datafiles."""
from pathlib import Path

import pyarrow
from faker import Faker
from pyarrow import parquet

TABLE_SCHEMAS = {
    'Customer': pyarrow.schema([
        pyarrow.field('c_custkey', pyarrow.long(), False),
        pyarrow.field('c_name', pyarrow.string(), False),
        pyarrow.field('c_address', pyarrow.string(), False),
        pyarrow.field('c_nationkey', pyarrow.long(), False),
        pyarrow.field('c_phone', pyarrow.string(), False),
        pyarrow.field('c_acctbal', pyarrow.float64(), False),
        pyarrow.field('c_acctbal', pyarrow.string(), False),
        pyarrow.field('c_comment', pyarrow.string(), False),
    ]),

    'Lineitem': pyarrow.schema([
        pyarrow.field('l_orderkey', pyarrow.long(), False),
        pyarrow.field('l_partkey', pyarrow.long(), False),
        pyarrow.field('l_suppkey', pyarrow.long(), False),
        pyarrow.field('l_linenumber', pyarrow.long(), True),
    ]),
    'subscriptions': pyarrow.schema([
        pyarrow.field('user_id', pyarrow.string(), False),
        pyarrow.field('channel_id', pyarrow.string(), False),
    ]),
    'streams': pyarrow.schema([
        pyarrow.field('stream_id', pyarrow.string(), False),
        pyarrow.field('channel_id', pyarrow.string(), False),
        pyarrow.field('name', pyarrow.string(), False),
    ]),
    'categories': pyarrow.schema([
        pyarrow.field('category_id', pyarrow.string(), False),
        pyarrow.field('name', pyarrow.string(), False),
        pyarrow.field('language', pyarrow.string(), False),
    ]),
    'watches': pyarrow.schema([
        pyarrow.field('user_id', pyarrow.string(), False),
        pyarrow.field('channel_id', pyarrow.string(), False),
        pyarrow.field('stream_id', pyarrow.string(), False),
        pyarrow.field('start_time', pyarrow.string(), False),
        pyarrow.field('end_time', pyarrow.string(), True),
    ]),
}


def get_mystream_schema(name: str) -> pyarrow.Schema:
    """Fetches the schema for the table with the requested name."""
    return TABLE_SCHEMAS[name]


# pylint: disable=fixme,line-too-long
def make_users_database():
    """Constructs the users table."""
    fake = Faker(['en_US'])
    rows = []
    # TODO -- Make the number of users, the uniqueness of userids, and the density of paid customers configurable.
    for _ in range(100):
        rows.append({'name': fake.name(),
                     'user_id': f'user{fake.unique.pyint(max_value=999999999):>09}',
                     'paid_for_service': fake.pybool(truth_probability=21)})
    table = pyarrow.Table.from_pylist(rows, schema=get_mystream_schema('users'))
    parquet.write_table(table, 'users.parquet', version='2.4', flavor='spark',
                        compression='NONE')


def create_mystream_database() -> Path:
    """Creates all the tables that make up the mystream database."""
    Faker.seed(9999)
    # Build all the tables in sorted order.
    make_users_database()
    return Path('users.parquet')
