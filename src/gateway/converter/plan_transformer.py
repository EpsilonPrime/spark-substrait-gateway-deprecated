# SPDX-License-Identifier: Apache-2.0
"""Routines to perform common transformations on Substrait plans."""
from typing import List

from gateway.converter.substrait_plan_visitor import SubstraitPlanVisitor


class PlanTransformer(SubstraitPlanVisitor):
    """Converts SparkConnect plans to Substrait plans."""

    def __init__(self):
        self._items: List[str] = []

        super().__init__()
