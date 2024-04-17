# SPDX-License-Identifier: Apache-2.0
"""Tests Substrait execution in the defined backends."""
from pathlib import Path

import ibis
import pytest
from ibis_substrait.compiler.core import SubstraitCompiler
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend as BackendEngine
from gateway.backends.backend_options import Backend, BackendOptions
from gateway.backends.backend_selector import find_backend

test_case_directory = Path(__file__).resolve().parent.parent.parent / 'tests' / 'data'

sql_test_case_paths = [f for f in sorted(test_case_directory.iterdir()) if f.suffix == '.sql']

sql_test_case_names = [p.stem for p in sql_test_case_paths]


@pytest.fixture(scope='function', params=[
    Backend.DUCKDB,
    Backend.DATAFUSION,
])
def backend(request):
    """The backend to use for testing."""
    options = BackendOptions(request.param, False)
    return find_backend(options)


def _register_table_for_duckdb(con, name: str, location: Path) -> None:
    con.read_parquet(location.glob('*.parquet'), table_name=name)


def _register_tpch_for_duckdb(con) -> None:
    """Registers the TPCH tables in Ibis."""
    location = BackendEngine.find_tpch()
    _register_table_for_duckdb(con, 'customer', location / 'customer')
    _register_table_for_duckdb(con, 'lineitem', location / 'lineitem')
    _register_table_for_duckdb(con, 'nation', location / 'nation')
    _register_table_for_duckdb(con, 'orders', location / 'orders')
    _register_table_for_duckdb(con, 'part', location / 'part')
    _register_table_for_duckdb(con, 'partsupp', location / 'partsupp')
    _register_table_for_duckdb(con, 'region', location / 'region')
    _register_table_for_duckdb(con, 'supplier', location / 'supplier')


def compile_sql(backend: BackendEngine, sql: str) -> plan_pb2.Plan:
    """Compiles SQL into a Substrait plan utilizing Ibis."""
    compiler = SubstraitCompiler()
    try:
        if backend.options.backend == Backend.DUCKDB:
            con = ibis.duckdb.connect()
            _register_tpch_for_duckdb(con)
            result = con.sql(sql)
            plan = compiler.compile(result)
        elif backend.options.backend == Backend.DATAFUSION:
            location = BackendEngine.find_tpch()
            method = 2
            if method == 1:
                con = ibis.datafusion.connect(
                    config={
                        'customer': location / 'customer' / 'part-0.parquet',
                        'lineitem': location / 'lineitem' / 'part-0.parquet',
                        'nation': location / 'nation' / 'part-0.parquet',
                        'orders': location / 'orders' / 'part-0.parquet',
                        'part': location / 'part' / 'part-0.parquet',
                        'partsupp': location / 'partsupp' / 'part-0.parquet',
                        'region': location / 'region' / 'part-0.parquet',
                        'supplier': location / 'supplier' / 'part-0.parquet',
                    }
                )
            elif method == 2:
                con = ibis.datafusion.connect()
                con.register(location / 'customer', 'customer')
                con.register(location / 'lineitem', 'lineitem')
                con.register(location / 'nation', 'nation')
                con.register(location / 'orders', 'orders')
                con.register(location / 'part', 'part')
                con.register(location / 'partsupp', 'partsupp')
                con.register(location / 'region', 'region')
                con.register(location / 'supplier', 'supplier')
            result = con.sql(sql)
            plan = compiler.compile(result)
        else:
            raise ValueError(f'Unknown backend: {backend}')
    except Exception as ex:
        raise ValueError(f'Ibis Compilation: {ex}')
    return plan


class TestSubstraitExecution:
    """Tests executing Substrait directly against the backends."""

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize(
        'path',
        sql_test_case_paths,
        ids=sql_test_case_names,
    )
    def test_tpch_substrait_execute(self, backend, path):
        """Test the TPC-H queries."""
        # Read the SQL to run.
        with open(path, "rb") as file:
            sql_bytes = file.read()
        sql = sql_bytes.decode('utf-8')

        plan = compile_sql(backend, sql)

        backend.execute(plan)
