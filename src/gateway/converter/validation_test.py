# SPDX-License-Identifier: Apache-2.0
"""Validation for the Spark to Substrait plan conversion routines."""
import os
from pathlib import Path

from google.protobuf import text_format
import pytest
from substrait.gen.proto import plan_pb2
import substrait_validator


test_case_directory = Path(os.path.dirname(os.path.realpath(__file__))) / 'data'

test_case_paths = [f for f in test_case_directory.iterdir() if f.name.endswith('.splan')]

test_case_names = [os.path.basename(p).removesuffix('.splan') for p in test_case_paths]


def find_diagnostic_message(issue):
    """Pulls the diagnostic message out of a substrait validator finding."""
    if issue.HasField('diagnostic'):
        return issue.diagnostic.msg
    if issue.HasField('child'):
        for data in issue.child.node.data:
            if data.HasField('diagnostic') and data.diagnostic.msg:
                return data.diagnostic.msg
            for data2 in data.child.node.data:
                if data2.HasField('diagnostic') and data2.diagnostic.msg:
                    return data2.diagnostic.msg
    return 'unknown recognized error, check substrait-validator output'


# pylint: disable=E1101,fixme
@pytest.mark.xfail
@pytest.mark.parametrize(
    'path',
    test_case_paths,
    ids=test_case_names,
)
def test_validate_substrait_plan(path):
    """Uses substrait-validator to check the plan for issues."""
    with open(path.with_suffix('.splan'), "rb") as file:
        splan_prototext = file.read()
    substrait_plan = text_format.Parse(splan_prototext, plan_pb2.Plan())
    parse_result = substrait_validator.parse_plan(substrait_plan.SerializeToString())
    issues = []
    for issue in parse_result.root.data:
        issues.append(find_diagnostic_message(issue))
    # TODO -- Stop treating warnings as errors for the purposes of this test.
    assert issues == []  # pylint: disable=use-implicit-booleaness-not-comparison
