# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
from collections import defaultdict
import operator
from typing import Dict

from substrait.gen.proto import plan_pb2
from substrait.gen.proto import algebra_pb2
from substrait.gen.proto.extensions import extensions_pb2

import spark.connect.base_pb2 as spark_pb2
import spark.connect.expressions_pb2 as spark_exprs_pb2
import spark.connect.relations_pb2 as spark_relations_pb2

from gateway.converter.spark_functions import ExtensionFunction, lookup_spark_function
from gateway.converter.symbol_table import PlanMetadata


# pylint: disable=E1101,fixme,too-many-public-methods
class SparkSubstraitConverter:
    """Converts SparkConnect plans to Substrait plans."""

    def __init__(self):
        self._function_uris: Dict[str, int] = {}
        self._functions: Dict[str, ExtensionFunction] = {}
        self._current_plan_id: int = None
        self._plan_metadata: Dict[int, PlanMetadata] = defaultdict(PlanMetadata)

    def lookup_function_by_name(self, name: str) -> int:
        """Finds the function reference for a given Spark function name."""
        if name in self._functions:
            return self._functions.get(name).anchor
        func = lookup_spark_function(name)
        if not func:
            raise LookupError(f'function name {name} does not have a known Substrait conversion')
        func.anchor = len(self._functions) + 1
        self._functions[name] = func
        if not self._function_uris.get(func.uri):
            self._function_uris[func.uri] = len(self._function_uris) + 1
        return self._functions.get(name).anchor

    def convert_boolean_literal(
            self, boolean: bool) -> algebra_pb2.Expression.Literal:
        """Transforms a boolean into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(boolean=boolean)

    def convert_short_literal(
            self, i: int) -> algebra_pb2.Expression.Literal:
        """Transforms a short integer into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(i16=i)

    def convert_integer_literal(
            self, i: int) -> algebra_pb2.Expression.Literal:
        """Transforms an integer into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(i32=i)

    def convert_float_literal(
            self, f: float) -> algebra_pb2.Expression.Literal:
        """Transforms a float into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(fp32=f)

    def convert_double_literal(
            self, d: float) -> algebra_pb2.Expression.Literal:
        """Transforms a double into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(fp64=d)

    def convert_string_literal(
            self, s: str) -> algebra_pb2.Expression.Literal:
        """Transforms a string into a Substrait expression literal."""
        return algebra_pb2.Expression.Literal(string=s)

    def convert_literal_expression(
            self, literal: spark_exprs_pb2.Expression.Literal) -> algebra_pb2.Expression:
        """Converts a Spark literal into a Substrait literal."""
        match literal.WhichOneof('literal_type'):
            case 'null':
                # TODO -- Finish with the type implementation.
                result = algebra_pb2.Expression.Literal()
            case 'binary':
                result = algebra_pb2.Expression.Literal()
            case 'boolean':
                result = self.convert_boolean_literal(literal.boolean)
            case 'byte':
                result = algebra_pb2.Expression.Literal()
            case 'short':
                result = self.convert_short_literal(literal.short)
            case 'integer':
                result = self.convert_integer_literal(literal.integer)
            case 'long':
                result = algebra_pb2.Expression.Literal()
            case 'float':
                result = self.convert_float_literal(literal.float)
            case 'double':
                result = self.convert_double_literal(literal.double)
            case 'decimal':
                result = algebra_pb2.Expression.Literal()
            case 'string':
                result = self.convert_string_literal(literal.string)
            case 'date':
                result = algebra_pb2.Expression.Literal()
            case 'timestamp':
                result = algebra_pb2.Expression.Literal()
            case 'timestamp_ntz':
                result = algebra_pb2.Expression.Literal()
            case 'calendar_interval':
                result = algebra_pb2.Expression.Literal()
            case 'year_month_interval':
                result = algebra_pb2.Expression.Literal()
            case 'day_time_interval':
                result = algebra_pb2.Expression.Literal()
            case 'array':
                result = algebra_pb2.Expression.Literal()
            case _:
                raise NotImplementedError(
                    f'Unexpected literal type: {literal.WhichOneof('literal_type')}')
        return algebra_pb2.Expression(literal=result)

    def convert_unresolved_attribute(
            self,
            _: spark_exprs_pb2.Expression.UnresolvedAttribute) -> algebra_pb2.Expression:
        """Converts a Spark unresolved attribute into a Substrait field reference."""
        # TODO -- Implement.
        return algebra_pb2.Expression(selection=algebra_pb2.Expression.FieldReference(
            direct_reference=algebra_pb2.Expression.ReferenceSegment(
                struct_field=algebra_pb2.Expression.ReferenceSegment.StructField(field=9999)),
            root_reference=algebra_pb2.Expression.FieldReference.RootReference()))

    def convert_unresolved_function(
            self,
            unresolved_function:
            spark_exprs_pb2.Expression.UnresolvedFunction) -> algebra_pb2.Expression:
        """Converts a Spark unresolved function into a Substrait scalar function."""
        func = algebra_pb2.Expression.ScalarFunction()
        func.function_reference = self.lookup_function_by_name(unresolved_function.function_name)
        for arg in unresolved_function.arguments:
            func.arguments.append(
                algebra_pb2.FunctionArgument(value=self.convert_expression(arg)))
        if unresolved_function.is_distinct:
            raise NotImplementedError(
                'Treating arguments as distinct is not supported for unresolved functions.')
        # TODO -- Calculate the output_type.
        return algebra_pb2.Expression(scalar_function=func)

    def convert_alias_expression(
            self, alias: spark_exprs_pb2.Expression.Alias) -> algebra_pb2.Expression:
        """Converts a Spark alias into a Substrait expression."""
        # TODO -- Utilize the alias name.
        return self.convert_expression(alias.expr)

    def convert_cast_expression(
            self, cast: spark_exprs_pb2.Expression.Cast) -> algebra_pb2.Expression:
        """Converts a Spark cast expression into a Substrait cast expression."""
        # TODO -- Implement type handling.
        return algebra_pb2.Expression(
            cast=algebra_pb2.Expression.Cast(input=self.convert_expression(cast.expr)))

    def convert_expression(self, expr: spark_exprs_pb2.Expression) -> algebra_pb2.Expression:
        """Converts a SparkConnect expression to a Substrait expression."""
        match expr.WhichOneof('expr_type'):
            case 'literal':
                result = self.convert_literal_expression(expr.literal)
            case 'unresolved_attribute':
                result = self.convert_unresolved_attribute(expr.unresolved_attribute)
            case 'unresolved_function':
                result = self.convert_unresolved_function(expr.unresolved_function)
            case 'expression_string':
                raise NotImplementedError(
                    'expression_string expression type not supported')
            case 'unresolved_star':
                raise NotImplementedError(
                    'unresolved_star expression type not supported')
            case 'alias':
                result = self.convert_alias_expression(expr.alias)
            case 'cast':
                result = self.convert_cast_expression(expr.cast)
            case 'unresolved_regex':
                raise NotImplementedError(
                    'unresolved_regex expression type not supported')
            case 'sort_order':
                raise NotImplementedError(
                    'sort_order expression type not supported')
            case 'lambda_function':
                raise NotImplementedError(
                    'lambda_function expression type not supported')
            case 'window':
                raise NotImplementedError(
                    'window expression type not supported')
            case 'unresolved_extract_value':
                raise NotImplementedError(
                    'unresolved_extract_value expression type not supported')
            case 'update_fields':
                raise NotImplementedError(
                    'update_fields expression type not supported')
            case 'unresolved_named_lambda_variable':
                raise NotImplementedError(
                    'unresolved_named_lambda_variable expression type not supported')
            case 'common_inline_user_defined_function':
                raise NotImplementedError(
                    'common_inline_user_defined_function expression type not supported')
            case _:
                raise NotImplementedError(
                    f'Unexpected expression type: {expr.WhichOneof("expr_type")}')
        return result

    def convert_expression_to_aggregate_function(
            self,
            _: spark_exprs_pb2.Expression) -> algebra_pb2.AggregateFunction:
        """Converts a SparkConnect expression to a Substrait expression."""
        # TODO -- Implement.
        return algebra_pb2.AggregateFunction()

    def convert_read_named_table_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read named table relation to a Substrait relation."""
        raise NotImplementedError('named tables are not yet implemented')

    def convert_read_data_source_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read data source relation into a Substrait relation."""
        local = algebra_pb2.ReadRel.LocalFiles()
        # TODO -- Handle the schema.
        for path in rel.paths:
            file_or_files = algebra_pb2.ReadRel.LocalFiles.FileOrFiles(uri_file=path)
            match rel.format:
                case 'parquet':
                    file_or_files.parquet.CopyFrom(
                        algebra_pb2.ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions())
                case 'orc':
                    file_or_files.orc.CopyFrom(
                        algebra_pb2.ReadRel.LocalFiles.FileOrFiles.OrcReadOptions())
                case 'text':
                    raise NotImplementedError('the only supported formats are parquet and orc')
                case 'json':
                    raise NotImplementedError('the only supported formats are parquet and orc')
                case 'csv':
                    # TODO -- Implement CSV once Substrait has support.
                    pass
                case 'avro':
                    raise NotImplementedError('the only supported formats are parquet and orc')
                case 'arrow':
                    file_or_files.parquet.CopyFrom(
                        algebra_pb2.ReadRel.LocalFiles.FileOrFiles.ArrowReadOptions())
                case 'dwrf':
                    file_or_files.parquet.CopyFrom(
                        algebra_pb2.ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions())
                case _:
                    raise NotImplementedError(f'Unexpected file format: {rel.format}')
            local.items.append(file_or_files)
        return algebra_pb2.Rel(read=algebra_pb2.ReadRel(local_files=local))

    def create_common_relation(self) -> algebra_pb2.RelCommon:
        """Creates the common metadata relation used by all relations."""
        return algebra_pb2.RelCommon(direct=algebra_pb2.RelCommon.Direct())

    def convert_read_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read relation into a Substrait relation."""
        match rel.WhichOneof('read_type'):
            case 'named_table':
                result = self.convert_read_named_table_relation(rel.named_table)
            case 'data_source':
                result = self.convert_read_data_source_relation(rel.data_source)
            case _:
                raise ValueError(f'Unexpected read type: {rel.WhichOneof("read_type")}')
        result.read.common.CopyFrom(self.create_common_relation())
        return result

    def convert_filter_relation(self, rel: spark_relations_pb2.Filter) -> algebra_pb2.Rel:
        """Converts a filter relation into a Substrait relation."""
        filter_rel = algebra_pb2.FilterRel(input=self.convert_relation(rel.input))
        filter_rel.common.CopyFrom(self.create_common_relation())
        filter_rel.condition.CopyFrom(self.convert_expression(rel.condition))
        return algebra_pb2.Rel(filter=filter_rel)

    def convert_sort_relation(self, rel: spark_relations_pb2.Sort) -> algebra_pb2.Rel:
        """Converts a sort relation into a Substrait relation."""
        sort = algebra_pb2.SortRel(input=self.convert_relation(rel.input))
        sort.common.CopyFrom(self.create_common_relation())
        for order in rel.order:
            if order.direction == spark_exprs_pb2.Expression.SortOrder.SORT_DIRECTION_ASCENDING:
                if order.null_ordering == spark_exprs_pb2.Expression.SortOrder.SORT_NULLS_FIRST:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_ASC_NULLS_FIRST
                else:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_ASC_NULLS_LAST
            else:
                if order.null_ordering == spark_exprs_pb2.Expression.SortOrder.SORT_NULLS_FIRST:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_DESC_NULLS_FIRST
                else:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_DESC_NULLS_LAST
            sort.sorts.append(algebra_pb2.SortField(
                expr=self.convert_expression(order.child),
                direction=direction))
        return algebra_pb2.Rel(sort=sort)

    def convert_limit_relation(self, rel: spark_relations_pb2.Limit) -> algebra_pb2.Rel:
        """Converts a limit relation into a Substrait FetchRel relation."""
        return algebra_pb2.Rel(
            fetch=algebra_pb2.FetchRel(common=self.create_common_relation(),
                                       input=self.convert_relation(rel.input), count=rel.limit))

    def convert_aggregate_relation(self, rel: spark_relations_pb2.Aggregate) -> algebra_pb2.Rel:
        """Converts an aggregate relation into a Substrait relation."""
        aggregate = algebra_pb2.AggregateRel(input=self.convert_relation(rel.input))
        aggregate.common.CopyFrom(self.create_common_relation())
        for grouping in rel.grouping_expressions:
            aggregate.groupings.append(
                algebra_pb2.AggregateRel.Grouping(
                    grouping_expressions=[self.convert_expression(grouping)]))
        for expr in rel.aggregate_expressions:
            aggregate.measures.append(
                algebra_pb2.AggregateRel.Measure(
                    measure=self.convert_expression_to_aggregate_function(expr))
            )
        return algebra_pb2.Rel(aggregate=aggregate)

    def convert_show_string_relation(self, rel: spark_relations_pb2.ShowString) -> algebra_pb2.Rel:
        """Converts a show string relation into a Substrait project relation."""
        # TODO -- Implement using num_rows, truncate, and vertical.
        return self.convert_relation(rel.input)

    def convert_with_columns_relation(
            self, rel: spark_relations_pb2.WithColumns) -> algebra_pb2.Rel:
        """Converts a with columns relation into a Substrait project relation."""
        project = algebra_pb2.ProjectRel(input=self.convert_relation(rel.input))
        project.common.CopyFrom(self.create_common_relation())
        num_emitted_fields = 0
        for alias in rel.aliases:
            # TODO -- Handle the output columns correctly.
            project.expressions.append(self.convert_expression(alias.expr))
            project.common.emit.output_mapping.append(num_emitted_fields)
        return algebra_pb2.Rel(project=project)

    def convert_relation(self, rel: spark_relations_pb2.Relation) -> algebra_pb2.Rel:
        """Converts a Spark relation into a Substrait one."""
        if self._current_plan_id is not None:
            self._plan_metadata[rel.common.plan_id].parent_plan_id = self._current_plan_id
        self._current_plan_id = rel.common.plan_id
        match rel.WhichOneof('rel_type'):
            case 'read':
                result = self.convert_read_relation(rel.read)
            case 'filter':
                result = self.convert_filter_relation(rel.filter)
            case 'sort':
                result = self.convert_sort_relation(rel.sort)
            case 'limit':
                result = self.convert_limit_relation(rel.limit)
            case 'aggregate':
                result = self.convert_aggregate_relation(rel.aggregate)
            case 'show_string':
                result = self.convert_show_string_relation(rel.show_string)
            case 'with_columns':
                result = self.convert_with_columns_relation(rel.with_columns)
            case _:
                raise ValueError(f'Unexpected rel type: {rel.WhichOneof("rel_type")}')
        return result

    def convert_plan(self, plan: spark_pb2.Plan) -> plan_pb2.Plan:
        """Converts a Spark plan into a Substrait plan."""
        result = plan_pb2.Plan()
        if plan.HasField('root'):
            result.relations.append(plan_pb2.PlanRel(
                root=algebra_pb2.RelRoot(input=self.convert_relation(plan.root))))
        for uri in sorted(self._function_uris.items(), key=operator.itemgetter(1)):
            result.extension_uris.append(
                extensions_pb2.SimpleExtensionURI(extension_uri_anchor=uri[1],
                                                  uri=uri[0]))
        for f in sorted(self._functions.values()):
            result.extensions.append(extensions_pb2.SimpleExtensionDeclaration(
                extension_function=extensions_pb2.SimpleExtensionDeclaration.ExtensionFunction(
                    extension_uri_reference=self._function_uris.get(f.uri),
                    function_anchor=f.anchor, name=f.name)))
        return result
