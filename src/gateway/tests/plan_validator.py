# SPDX-License-Identifier: Apache-2.0
from contextlib import contextmanager

import pytest
import substrait_validator
from google.protobuf import json_format
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from substrait.gen.proto import plan_pb2


def validate_plan(json_plan: str):
    substrait_plan = json_format.Parse(json_plan, plan_pb2.Plan())
    diagnostics = substrait_validator.plan_to_diagnostics(substrait_plan.SerializeToString())
    issues = []
    for issue in diagnostics:
        if issue.adjusted_level >= substrait_validator.Diagnostic.LEVEL_ERROR:
            issues.append([issue.msg, substrait_validator.path_to_string(issue.path)])
    if issues:
        issues_as_text = '\n'.join(f'  → {issue[0]}\n    at {issue[1]}' for issue in issues)
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
    try:
        exception = None
        yield
    except SparkConnectGrpcException as e:
        exception = e
    if session.conf.get('spark-substrait-gateway.backend', 'spark') == 'spark':
        raise exception
    plan_count = int(session.conf.get('spark-substrait-gateway.plan_count'))
    plans_as_text = []
    for i in range(plan_count):
        plan = session.conf.get(f'spark-substrait-gateway.plan.{i + 1}')
        plans_as_text.append( f'Plan #{i+1}:\n{plan}\n')
        validate_plan(plan)
    if exception:
        pytest.fail(f'Exception raised during execution: {exception.message}\n\n' +
                    '\n\n'.join(plans_as_text), pytrace=False)
