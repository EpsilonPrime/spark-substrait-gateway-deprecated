# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from typing import Any, List, Tuple

from substrait.gen.proto import algebra_pb2, plan_pb2

from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor


# pylint: disable=no-member
class SimplifyCasts(SubstraitPlanVisitor):
    """Replaces all cast expressions with projects of casts instead."""
    _pass: int
    _current_relation: algebra_pb2.Rel
    _rewrite_expressions: List[algebra_pb2.Expression]

    def __init__(self):
        super().__init__()
        self._rewrite_expressions: List[algebra_pb2.Expression] = []
        self._previous_rewrite_expressions: List[List[algebra_pb2.Expression]] = []

    def visit_cast(self, cast: algebra_pb2.Expression.Cast) -> Any:
        """Visits a cast node."""
        super().visit_cast(cast)
        match self._pass:
            case 1:
                # TODO -- Also add literal here.
                if cast.input.WhichOneof('rex_type') != 'selection':
                    self._rewrite_expressions.append(cast.input)
            case 2:
                # Rewrite the cast to use a selection of our new projection.
                field_reference = 9999
                cast.input.CopyFrom(algebra_pb2.Expression(algebra_pb2.Expression.FieldReference(
                    direct_reference=algebra_pb2.Expression.ReferenceSegment(
                        struct_field=algebra_pb2.Expression.ReferenceSegment.StructField(
                            field=field_reference)),
                    root_reference=algebra_pb2.Expression.FieldReference.RootReference()))
                )

    @staticmethod
    def find_single_input(rel: algebra_pb2.Rel) -> algebra_pb2.Rel:
        """Finds the single input to the relation."""
        match rel.WhichOneof('rel_type'):
            case 'filter':
                return rel.filter.input
            case 'fetch':
                return rel.fetch.input
            case 'aggregate':
                return rel.aggregate.input
            case 'sort':
                return rel.sort.input
            case 'project':
                return rel.project.input
            case 'extension_single':
                return rel.extension_single.input
            case _:
                raise NotImplementedError('Finding single inputs of relations with type '
                                          f'{rel.WhichOneof('rel_type')} are not implemented')

    @staticmethod
    def replace_single_input(rel: algebra_pb2.Rel, new_input: algebra_pb2.Rel):
        """Updates the single input to the relation."""
        match rel.WhichOneof('rel_type'):
            case 'filter':
                rel.filter.input.CopyFrom(new_input)
            case 'fetch':
                rel.fetch.input.CopyFrom(new_input)
            case 'aggregate':
                rel.aggregate.input.CopyFrom(new_input)
            case 'sort':
                rel.sort.input.CopyFrom(new_input)
            case 'project':
                rel.project.input.CopyFrom(new_input)
            case 'extension_single':
                rel.extension_single.input.CopyFrom(new_input)
            case _:
                raise NotImplementedError('Modifying inputs of relations with type '
                                          f'{rel.WhichOneof('rel_type')} are not implemented')

    def visit_relation(self, rel: algebra_pb2.Rel) -> Any:
        """Visits a relation node."""
        self._previous_rewrite_expressions.append(self._rewrite_expressions)
        self._rewrite_expressions = []
        # Go through the first time finding any casts that need rewrites.
        self._pass = 1
        super().visit_relation(rel)
        if self._rewrite_expressions:
            old_input = self.find_single_input(rel)
            # TODO -- Add the common rel.
            new_input = algebra_pb2.Rel(
                project=algebra_pb2.ProjectRel(input=old_input))
            for expr in self._rewrite_expressions:
                new_input.project.expressions.append(expr)
            self.replace_single_input(rel, new_input)
            self._pass = 2
            # Go through a second time rewriting the casts.
            super().visit_relation(rel)
        self._rewrite_expressions = self._previous_rewrite_expressions.pop()
