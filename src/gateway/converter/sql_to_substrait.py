# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
from gateway.backends import backend_selector
from gateway.backends.backend_options import Backend, BackendOptions
from gateway.converter.add_extension_uris import AddExtensionUris
from substrait.gen.proto import plan_pb2


def convert_sql(sql: str, backend=None) -> plan_pb2.Plan:
    """Convert SQL into a Substrait plan."""
    plan = plan_pb2.Plan()

    # If the SQL is a CREATE statement, use the backend to create the table.
    if "CREATE" in sql:
        connection = backend.get_connection()
        connection.execute(sql)
        return

    backend = backend_selector.find_backend(BackendOptions(Backend.DUCKDB, False))
    backend.register_tpch()
    connection = backend.get_connection()
    proto_bytes = connection.get_substrait(query=sql).fetchone()[0]
    plan.ParseFromString(proto_bytes)

    # TODO -- Remove this after the SQL converter is fixed.
    AddExtensionUris().visit_plan(plan)

    return plan
