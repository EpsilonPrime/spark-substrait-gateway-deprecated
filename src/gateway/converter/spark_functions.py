# SPDX-License-Identifier: Apache-2.0
"""Provides the mapping of Spark functions to Substrait."""
import dataclasses
from typing import Optional

from substrait.gen.proto import type_pb2


@dataclasses.dataclass
class ExtensionFunction:
    """Represents a Substrait function."""
    uri: str
    name: str
    output_type: type_pb2.Type
    anchor: int
    max_args: Optional[int]

    def __init__(self, uri: str, name: str, type: type_pb2.Type,
                 max_args: Optional[int] = None):
        self.uri = uri
        self.name = name
        self.output_type = type
        self.max_args = max_args

    def __lt__(self, obj):
        return ((self.uri) < (obj.uri) and (self.name) < (obj.name))


SPARK_SUBSTRAIT_MAPPING = {
    'split': ExtensionFunction(
        '/functions_string.yaml', 'string_split:str_str', type_pb2.Type(
            string=type_pb2.Type.String(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED)),
        max_args=2),
    '==': ExtensionFunction(
        '/functions_comparison.yaml', 'equal:str_str', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'array_contains': ExtensionFunction(
        '/functions_set.yaml', 'index_in:str_liststr', type_pb2.Type(
            bool=type_pb2.Type.Boolean(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED))),
    'sum': ExtensionFunction(
        '/functions_arithmetic.yaml', 'sum:int', type_pb2.Type(
            i32=type_pb2.Type.I32(
                nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED)))
}


def lookup_spark_function(name: str) -> ExtensionFunction:
    """Returns a Substrait function given a spark function name."""
    return SPARK_SUBSTRAIT_MAPPING.get(name)
