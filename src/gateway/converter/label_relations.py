# SPDX-License-Identifier: Apache-2.0
"""A library to search Substrait plan for local files."""
from typing import Any, List

from substrait.gen.proto import algebra_pb2

from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor


def get_common_section(rel: algebra_pb2.Rel) -> algebra_pb2.RelCommon:
    """Finds the single input to the relation."""
    match rel.WhichOneof('rel_type'):
        case 'read':
            return rel.read.common
        case 'filter':
            return rel.filter.common
        case 'fetch':
            return rel.fetch.common
        case 'aggregate':
            return rel.aggregate.common
        case 'sort':
            return rel.sort.common
        case 'project':
            return rel.project.common
        case 'extension_single':
            return rel.extension_single.common
        case _:
            raise NotImplementedError('Finding the common section for type '
                                      f'{rel.WhichOneof('rel_type')} is not implemented')


# pylint: disable=no-member
class LabelRelations(SubstraitPlanVisitor):
    """Replaces all cast expressions with projects of casts instead."""
    _seen_relations: int

    def __init__(self):
        super().__init__()
        self._seen_relations = 0

    def visit_relation(self, rel: algebra_pb2.Rel) -> Any:
        """Visits a relation node."""
        label = algebra_pb2.ReferenceRel(subtree_ordinal=self._seen_relations)
        get_common_section(rel).advanced_extension.optimization.Pack(label)
        self._seen_relations += 1
        super().visit_relation(rel)


# pylint: disable=no-member
class UnlabelRelations(SubstraitPlanVisitor):
    """Removes all labels created by LabelRelations from relations."""

    def __init__(self):
        super().__init__()

    def visit_relation(self, rel: algebra_pb2.Rel) -> Any:
        """Visits a relation node."""
        get_common_section(rel).ClearField('advanced_extension')
        super().visit_relation(rel)
