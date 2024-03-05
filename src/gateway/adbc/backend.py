# SPDX-License-Identifier: Apache-2.0
"""Provides client access to an ADBC backend."""
import adbc_driver_duckdb.dbapi

from substrait.gen.proto import plan_pb2


# pylint: disable=too-few-public-methods
class AdbcBackend:
    """Provides methods for contacting an ADBC backend via Substrait."""

    def __init__(self):
        pass

    def execute(self, plan: 'plan_pb2.Plan') -> str:
        """Executes the given Substrait plan against the associated ADBC backend."""
        with adbc_driver_duckdb.dbapi.connect() as conn, conn.cursor() as cur:
            plan_data = plan.SerializeToString()
            cur.adbc_statement.set_substrait_plan(plan_data)
            tbl = cur.fetch_arrow_table()
            return f'{tbl}'
