# SPDX-License-Identifier: Apache-2.0
"""Converts the provided plans from the DuckDB Substrait dialect to Acero's."""
import sys

from google.protobuf import json_format
from substrait.gen.proto import plan_pb2

from gateway.converter.label_relations import LabelRelations, UnlabelRelations
from gateway.converter.output_field_tracking_visitor import OutputFieldTrackingVisitor
from gateway.converter.simplify_casts import SimplifyCasts


# pylint: disable=E1101
def main():
    """Converts the provided plans from the DuckDB Substrait dialect to Acero's."""
    args = sys.argv[1:]
    if len(args) != 2:
        print("Usage: python duckdb_substrait_to_arrow.py <path to plan> <path to output plan>")
        sys.exit(1)

    with open(args[0], "rb") as file:
        plan_prototext = file.read()
    duckdb_plan = json_format.Parse(plan_prototext, plan_pb2.Plan())

    arrow_plan = duckdb_plan
    LabelRelations().visit_plan(arrow_plan)
    symbol_table = OutputFieldTrackingVisitor().visit_plan(arrow_plan)
    SimplifyCasts(symbol_table).visit_plan(arrow_plan)
    UnlabelRelations().visit_plan(arrow_plan)

    with open(args[1], "wt", encoding='utf-8') as file:
        file.write(json_format.MessageToJson(arrow_plan))


if __name__ == '__main__':
    main()
