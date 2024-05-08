# SPDX-License-Identifier: Apache-2.0
from contextlib import contextmanager

import pytest
import substrait_validator
from google.protobuf import json_format
from substrait.gen.proto import plan_pb2


def validate_plan(json_plan: str):
    substrait_plan = json_format.Parse(json_plan, plan_pb2.Plan())
    diagnostics = substrait_validator.plan_to_diagnostics(substrait_plan.SerializeToString())
    issues = []
    for issue in diagnostics:
        if issue.adjusted_level >= substrait_validator.Diagnostic.LEVEL_ERROR:
            issues.append(issue.msg)
    if issues:
        issues_as_text = '\n'.join(f'  → {issue}' for issue in issues)
        pytest.fail(f'Validation failed.  Issues:\n{issues_as_text}\n\nPlan:\n{substrait_plan}\n',
                    pytrace=False)


@contextmanager
def utilizes_valid_plans(session):
    """Validates that the plans used by the gateway backend pass validation."""
    if hasattr(session, 'sparkSession'):
        session = session.sparkSession
    # Reset the statistics, so we only see the plans that were created during our lifetime.
    if session.conf.get('spark-substrait-gateway.backend', 'spark') != 'spark':
        session.conf.set('spark-substrait-gateway.reset_statistics', None)
    yield
    if session.conf.get('spark-substrait-gateway.backend', 'spark') == 'spark':
        return
    plan_count = int(session.conf.get('spark-substrait-gateway.plan_count'))
    for i in range(plan_count):
        plan = session.conf.get(f'spark-substrait-gateway.plan.{i + 1}')
        validate_plan(plan)


def dump_plans(session, printer):
    if session.conf.get('spark-substrait-gateway.backend', 'spark') == 'spark':
        return
    plan_count = int(session.conf.get('spark-substrait-gateway.plan_count'))
    for i in range(plan_count):
        plan = session.conf.get(f'spark-substrait-gateway.plan.{i + 1}')
        printer(f'Plan {i + 1}:\n{plan}\n')
