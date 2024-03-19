# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from typing import Any, List, Optional

from substrait.gen.proto import algebra_pb2

from gateway.converter.label_relations import get_common_section
from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor
from gateway.converter.symbol_table import SymbolTable


def get_plan_id_from_common(common: algebra_pb2.RelCommon) -> int:
    ref_rel = algebra_pb2.ReferenceRel()
    common.advanced_extension.optimization.Unpack(ref_rel)
    return ref_rel.subtree_ordinal


def get_plan_id(rel: algebra_pb2.Rel) -> int:
    common = get_common_section(rel)
    return get_plan_id_from_common(common)


# pylint: disable=no-member,fixme
class SimplifyCasts(SubstraitPlanVisitor):
    """Replaces all cast expressions with projects of casts instead."""
    _pass: int  # TODO -- Investigate removing the need for multiple passes.
    _current_relation: algebra_pb2.Rel
    _rewrite_expressions: List[algebra_pb2.Expression]

    def __init__(self):
        super().__init__()
        self._current_plan_id: Optional[int] = None  # The relation currently being processed.
        self._symbol_table = SymbolTable()

        self._seen_casts = None
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
                symbol = self._symbol_table.get_symbol(self._current_plan_id)
                field_reference = len(symbol.input_fields) + self._seen_casts
                self._seen_casts += 1
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

    def update_field_references(self, plan_id: int) -> None:
        """Uses the field references using the specified portion of the plan."""
        source_symbol = self._symbol_table.get_symbol(plan_id)
        current_symbol = self._symbol_table.get_symbol(self._current_plan_id)
        current_symbol.input_fields.extend(source_symbol.output_fields)
        current_symbol.output_fields.extend(current_symbol.input_fields)

    def visit_read_relation(self, rel: algebra_pb2.ReadRel) -> Any:
        """Uses the field references from the read relation."""
        result = super().visit_read_relation(rel)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        # TODO -- Validate this logic.
        for field in rel.base_schema.names:
            symbol.output_fields.append(field)
        return result

    def visit_filter_relation(self, rel: algebra_pb2.FilterRel) -> Any:
        result = super().visit_filter_relation(rel)
        self.update_field_references(get_plan_id_from_common(rel.common))
        return result

    def visit_fetch_relation(self, fetch: algebra_pb2.FetchRel) -> Any:
        result = super().visit_fetch_relation(fetch)
        self.update_field_references(get_plan_id_from_common(fetch.common))
        return result

    def visit_aggregate_relation(self, aggregate: algebra_pb2.AggregateRel) -> Any:
        result = super().visit_aggregate_relation(aggregate)
        self.update_field_references(get_plan_id_from_common(aggregate.common))
        return result

    def visit_sort_relation(self, sort: algebra_pb2.SortRel) -> Any:
        result = super().visit_sort_relation(sort)
        self.update_field_references(get_plan_id_from_common(sort.common))
        return result

    def visit_project_relation(self, project: algebra_pb2.ProjectRel) -> Any:
        result = super().visit_project_relation(project)
        self.update_field_references(get_plan_id_from_common(project.common))
        return result

    def visit_extension_single_relation(self, rel: algebra_pb2.ExtensionSingleRel) -> Any:
        result = super().visit_extension_single_relation(rel)
        self.update_field_references(get_plan_id_from_common(rel.common))
        return result

    # TODO -- Add the other relation types.

    def visit_relation(self, rel: algebra_pb2.Rel) -> Any:
        """Visits a relation node."""
        new_plan_id = get_plan_id(rel)
        self._symbol_table.add_symbol(new_plan_id,
                                      parent=self._current_plan_id,
                                      symbol_type=rel.WhichOneof('rel_type'))
        old_plan_id = self._current_plan_id
        self._current_plan_id = new_plan_id

        self._previous_rewrite_expressions.append(self._rewrite_expressions)
        self._rewrite_expressions = []
        # Go through the first time finding any casts that need rewrites.
        self._pass = 1
        super().visit_relation(rel)
        if self._rewrite_expressions:
            old_input = self.find_single_input(rel)
            new_input = algebra_pb2.Rel(
                project=algebra_pb2.ProjectRel(
                    common=algebra_pb2.RelCommon(direct=algebra_pb2.RelCommon.Direct()),
                    input=old_input))
            for expr in self._rewrite_expressions:
                new_input.project.expressions.append(expr)
            self.replace_single_input(rel, new_input)
            self._pass = 2
            # Go through a second time rewriting the casts.
            self._seen_casts = 0
            super().visit_relation(rel)
        self._rewrite_expressions = self._previous_rewrite_expressions.pop()

        self._current_plan_id = old_plan_id
