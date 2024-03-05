# SPDX-License-Identifier: Apache-2.0
"""Will eventually provide client access to an ADBC backend."""
import duckdb

from substrait.gen.proto import plan_pb2


# pylint: disable=too-few-public-methods
class AdbcBackend:
    """Provides methods for contacting an ADBC backend via Substrait."""

    def __init__(self):
        pass

    def execute(self, plan: 'plan_pb2.Plan') -> str:
        """Executes the given Substrait plan against duckdb."""
        con = duckdb.connect()
        con.install_extension('substrait')
        con.load_extension('substrait')
        plan_data = plan.SerializeToString()
        query_result = con.from_substrait(proto=plan_data)
        return f'{query_result}'
