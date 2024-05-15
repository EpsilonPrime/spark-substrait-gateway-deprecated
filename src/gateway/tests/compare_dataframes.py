# SPDX-License-Identifier: Apache-2.0
"""Routines for comparing dataframes."""
import datetime

from pyspark import Row
from pyspark.testing import assertDataFrameEqual


def align_schema(source_df: list[Row], schema_df: list[Row]):
    """Returns a copy of source_df with the fields changed to match schema_df."""
    schema = schema_df[0]

    new_source_df = []
    for row in source_df:
        new_row = {}
        for field_name, field_value in schema.asDict().items():
            if isinstance(field_value, datetime.date):
                new_row[field_name] = row[field_name].date()
            else:
                new_row[field_name] = row[field_name]

        new_source_df.append(Row(**new_row))

    return new_source_df


def assert_dataframes_equal(outcome, expected):
    # Create a copy of the dataframes to avoid modifying the original ones
    modified_outcome = align_schema(outcome, expected)

    assertDataFrameEqual(modified_outcome, expected, atol=1e-2)
