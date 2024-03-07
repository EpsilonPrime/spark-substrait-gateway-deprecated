# SPDX-License-Identifier: Apache-2.0
"""Will eventually provide client access to an ADBC backend."""
import duckdb
import pyarrow
import pyarrow.substrait as substrait
from datafusion import SessionContext
from datafusion import substrait as ds

from substrait.gen.proto import plan_pb2


# pylint: disable=fixme
class AdbcBackend:
    """Provides methods for contacting an ADBC backend via Substrait."""

    def __init__(self):
        pass

    def execute_with_duckdb(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against DuckDB."""
        con = duckdb.connect()
        con.install_extension('substrait')
        con.load_extension('substrait')
        plan_data = plan.SerializeToString()
        query_result = con.from_substrait(proto=plan_data)
        df = query_result.df()
        return pyarrow.Table.from_pandas(df=df)

    def execute_with_arrow(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against Acero."""
        plan_data = plan.SerializeToString()
        reader = pyarrow.substrait.run_query(plan_data)
        query_result = reader.read_all()
        return query_result

    def execute_with_datafusion(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan against Datafusion."""
        ctx = SessionContext()
        ctx.register_parquet("demotable", '/Users/davids/Desktop/artists.parquet')

        plan_data = plan.SerializeToString()
        substrait_plan = ds.substrait.serde.deserialize_bytes(plan_data)
        logical_plan = ds.substrait.consumer.from_substrait_plan(
            ctx, substrait_plan
        )

        # Create a DataFrame from a deserialized logical plan
        df_result = ctx.create_dataframe_from_logical_plan(logical_plan)
        return df_result.to_arrow_table()

    def execute(self, plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
        """Executes the given Substrait plan."""
        # TODO -- Make configurable.
        return self.execute_with_duckdb(plan)
