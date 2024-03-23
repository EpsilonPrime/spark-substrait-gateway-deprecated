# SPDX-License-Identifier: Apache-2.0
"""Convenience builder for constructing Substrait plans."""
from typing import List

from substrait.gen.proto import algebra_pb2, type_pb2

from gateway.converter.spark_functions import ExtensionFunction


# pylint: disable=E1101

def fetch_relation(input_relation: algebra_pb2.Rel, num_rows: int) -> algebra_pb2.Rel:
    """Constructs a Substrait fetch plan node."""
    fetch = algebra_pb2.Rel(fetch=algebra_pb2.FetchRel(input=input_relation, count=num_rows))

    return fetch


def project_relation(input_relation: algebra_pb2.Rel,
                     expressions: List[algebra_pb2.Expression]) -> algebra_pb2.Rel:
    """Constructs a Substrait project plan node."""
    return algebra_pb2.Rel(
        project=algebra_pb2.ProjectRel(input=input_relation, expressions=expressions))


# pylint: disable=fixme
def aggregate_relation(input_relation: algebra_pb2.Rel,
                       measures: List[algebra_pb2.AggregateFunction]) -> algebra_pb2.Rel:
    """Constructs a Substrait aggregate plan node."""
    aggregate = algebra_pb2.Rel(
        aggregate=algebra_pb2.AggregateRel(
            common=algebra_pb2.RelCommon(emit=algebra_pb2.RelCommon.Emit(
                output_mapping=range(len(measures)))),
            input=input_relation))
    # TODO -- Add support for groupings.
    for measure in measures:
        aggregate.aggregate.measures.append(
            algebra_pb2.AggregateRel.Measure(measure=measure))
    return aggregate


def join_relation(left: algebra_pb2.Rel, right: algebra_pb2.Rel) -> algebra_pb2.Rel:
    """Constructs a Substrait join plan node."""
    return algebra_pb2.Rel(
        join=algebra_pb2.JoinRel(common=algebra_pb2.RelCommon(), left=left, right=right,
                                 expression=algebra_pb2.Expression(
                                     literal=algebra_pb2.Expression.Literal(boolean=True)),
                                 type=algebra_pb2.JoinRel.JoinType.JOIN_TYPE_INNER))


def concat(function_info: ExtensionFunction,
           *expressions: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Constructs a Substrait concat expression."""
    return algebra_pb2.Expression(
        scalar_function=algebra_pb2.Expression.ScalarFunction(
            function_reference=function_info.anchor,
            output_type=function_info.output_type,
            arguments=[algebra_pb2.FunctionArgument(value=expression) for expression in expressions]
        ))


def strlen(function_info: ExtensionFunction,
           expression: algebra_pb2.Expression) -> algebra_pb2.Expression:
    """Constructs a Substrait concat expression."""
    return algebra_pb2.Expression(
        scalar_function=algebra_pb2.Expression.ScalarFunction(
            function_reference=function_info.anchor,
            output_type=function_info.output_type,
            arguments=[algebra_pb2.FunctionArgument(value=expression)]))


def cast(expression: algebra_pb2.Expression, output_type: type_pb2.Type) -> algebra_pb2.Expression:
    """Constructs a Substrait cast expression."""
    return algebra_pb2.Expression(
        cast=algebra_pb2.Expression.Cast(input=expression, type=output_type)
    )


def field_reference(field_number: int) -> algebra_pb2.Expression:
    """Constructs a Substrait field reference expression."""
    return algebra_pb2.Expression(
        selection=algebra_pb2.Expression.FieldReference(
            direct_reference=algebra_pb2.Expression.ReferenceSegment(
                struct_field=algebra_pb2.Expression.ReferenceSegment.StructField(
                    field=field_number))))


def max_function(function_info: ExtensionFunction,
        field_number: int) -> algebra_pb2.AggregateFunction:
    """Constructs a Substrait concat expression."""
    return algebra_pb2.AggregateFunction(
        function_reference=function_info.anchor,
        output_type=function_info.output_type,
        arguments=[algebra_pb2.FunctionArgument(value=field_reference(field_number))])


def string_type(required: bool = True) -> type_pb2.Type:
    """Constructs a Substrait string type."""
    if required:
        nullability = type_pb2.Type.Nullability.NULLABILITY_REQUIRED
    else:
        nullability = type_pb2.Type.Nullability.NULLABILITY_NULLABLE
    return type_pb2.Type(string=type_pb2.Type.String(nullability=nullability))
