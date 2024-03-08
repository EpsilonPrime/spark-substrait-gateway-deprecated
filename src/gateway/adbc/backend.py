# SPDX-License-Identifier: Apache-2.0
"""Will eventually provide client access to an ADBC backend."""
import duckdb
import pyarrow
from pyarrow import substrait
from datafusion import SessionContext
from datafusion import substrait as ds

from substrait.gen.proto import plan_pb2

from gateway.adbc.backend_options import BackendOptions, Backend


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
        reader = substrait.run_query(plan_data)
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

    def execute(self, plan: 'plan_pb2.Plan', options: BackendOptions) -> pyarrow.lib.Table:
        """Executes the given Substrait plan."""
        match options.backend:
            case Backend.ARROW:
                return self.execute_with_arrow(plan)
            case Backend.DATAFUSION:
                return self.execute_with_datafusion(plan)
            case Backend.DUCKDB:
                return self.execute_with_duckdb(plan)
            case _:
                raise ValueError('unknown backend requested')
