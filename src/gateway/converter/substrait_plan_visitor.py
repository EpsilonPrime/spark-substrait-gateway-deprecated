# SPDX-License-Identifier: Apache-2.0
"""Abstract visitor class for Substrait plans."""
from typing import Dict, Optional

from substrait.gen.proto import plan_pb2
from substrait.gen.proto import algebra_pb2
from substrait.gen.proto import type_pb2
from substrait.gen.proto.extensions import extensions_pb2


# pylint: disable=E1101,fixme,too-many-public-methods
class SubstraitPlanVisitor:
    """Base class that visits all the parts of a Substrait plan."""

    def visit_literal_expression(
            self, literal: algebra_pb2.Expression.Literal) -> None:
        """Converts a Spark literal into a Substrait literal."""
        pass


    def visit_alias_expression(
            self, alias: spark_exprs_pb2.Expression.Alias) -> algebra_pb2.Expression:
        """Converts a Spark alias into a Substrait expression."""
        # TODO -- Utilize the alias name.
        return self.visit_expression(alias.expr)

    def visit_type_str(self, spark_type_str: Optional[str]) -> type_pb2.Type:
        """Converts a Spark type string into a Substrait type."""
        # TODO -- Properly handle nullability.
        match spark_type_str:
            case 'boolean':
                return type_pb2.Type(bool=type_pb2.Type.Boolean(
                    nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED
                ))
            case 'integer':
                return type_pb2.Type(i32=type_pb2.Type.I32(
                    nullability=type_pb2.Type.Nullability.NULLABILITY_REQUIRED
                ))
            # TODO -- Add all of the other types.
            case _:
                raise NotImplementedError(
                    f'type {spark_type_str} not yet implemented.')

    def visit_type(self, spark_type: spark_types_pb2.DataType) -> type_pb2.Type:
        """Converts a Spark type into a Substrait type."""
        return self.visit_type_str(spark_type.WhichOneof('kind'))

    def visit_cast_expression(
            self, cast: spark_exprs_pb2.Expression.Cast) -> algebra_pb2.Expression:
        """Converts a Spark cast expression into a Substrait cast expression."""
        cast_rel = algebra_pb2.Expression.Cast(input=self.visit_expression(cast.expr))
        match cast.WhichOneof('cast_to_type'):
            case 'type':
                cast_rel.type.CopyFrom(self.visit_type(cast.type))
            case 'type_str':
                cast_rel.type.CopyFrom(self.visit_type_str(cast.type_str))
            case _:
                raise NotImplementedError(
                    f'unknown cast_to_type {cast.WhichOneof('cast_to_type')}'
                )
        return algebra_pb2.Expression(cast=cast_rel)

    def visit_expression(self, expr: spark_exprs_pb2.Expression) -> algebra_pb2.Expression:
        """Converts a SparkConnect expression to a Substrait expression."""
        match expr.WhichOneof('expr_type'):
            case 'literal':
                result = self.visit_literal_expression(expr.literal)
            case 'unresolved_attribute':
                result = self.visit_unresolved_attribute(expr.unresolved_attribute)
            case 'unresolved_function':
                result = self.visit_unresolved_function(expr.unresolved_function)
            case 'expression_string':
                raise NotImplementedError(
                    'expression_string expression type not supported')
            case 'unresolved_star':
                raise NotImplementedError(
                    'unresolved_star expression type not supported')
            case 'alias':
                result = self.visit_alias_expression(expr.alias)
            case 'cast':
                result = self.visit_cast_expression(expr.cast)
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

    def visit_expression_to_aggregate_function(
            self,
            expr: spark_exprs_pb2.Expression) -> algebra_pb2.AggregateFunction:
        """Converts a SparkConnect expression to a Substrait expression."""
        func = algebra_pb2.AggregateFunction()
        expression = self.visit_expression(expr)
        match expression.WhichOneof('rex_type'):
            case 'scalar_function':
                function = expression.scalar_function
            case 'window_function':
                function = expression.window_function
            case _:
                raise NotImplementedError(
                    'only functions of type unresolved function are supported in aggregate '
                    'relations')
        func.function_reference = function.function_reference
        func.arguments.extend(function.arguments)
        func.options.extend(function.options)
        func.output_type.CopyFrom(function.output_type)
        return func

    def visit_read_named_table_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read named table relation to a Substrait relation."""
        raise NotImplementedError('named tables are not yet implemented')

    def visit_schema(self, schema_str: str) -> Optional[type_pb2.NamedStruct]:
        """Converts the Spark JSON schema string into a Subtrait named type structure."""
        if not schema_str:
            return None
        # TODO -- Deal with potential denial of service due to malformed JSON.
        schema_data = json.loads(schema_str)
        schema = type_pb2.NamedStruct()
        schema.struct.nullability = type_pb2.Type.NULLABILITY_REQUIRED
        for field in schema_data.get('fields'):
            schema.names.append(field.get('name'))
            if field.get('nullable'):
                nullability = type_pb2.Type.NULLABILITY_NULLABLE
            else:
                nullability = type_pb2.Type.NULLABILITY_REQUIRED
            match field.get('type'):
                case 'boolean':
                    field_type = type_pb2.Type(bool=type_pb2.Type.Boolean(nullability=nullability))
                case 'short':
                    field_type = type_pb2.Type(i16=type_pb2.Type.I16(nullability=nullability))
                case 'integer':
                    field_type = type_pb2.Type(i32=type_pb2.Type.I32(nullability=nullability))
                case 'long':
                    field_type = type_pb2.Type(i64=type_pb2.Type.I64(nullability=nullability))
                case 'string':
                    field_type = type_pb2.Type(string=type_pb2.Type.String(nullability=nullability))
                case _:
                    raise NotImplementedError(f'Unexpected field type: {field.get("type")}')

            schema.struct.types.append(field_type)
        return schema

    def visit_read_data_source_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read data source relation into a Substrait relation."""
        local = algebra_pb2.ReadRel.LocalFiles()
        schema = self.visit_schema(rel.schema)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        for field_name in schema.names:
            symbol.output_fields.append(field_name)
        if self._conversion_options.use_named_table_workaround:
            return algebra_pb2.Rel(
                read=algebra_pb2.ReadRel(base_schema=schema,
                                         named_table=algebra_pb2.ReadRel.NamedTable(
                                             names=['demotable'])))
        for path in rel.paths:
            uri_path = path
            if self._conversion_options.needs_scheme_in_path_uris:
                if uri_path.startswith('/'):
                    uri_path = "file:" + uri_path
            file_or_files = algebra_pb2.ReadRel.LocalFiles.FileOrFiles(uri_file=uri_path)
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
        return algebra_pb2.Rel(read=algebra_pb2.ReadRel(base_schema=schema, local_files=local))

    def create_common_relation(self) -> algebra_pb2.RelCommon:
        """Creates the common metadata relation used by all relations."""
        if not self._conversion_options.use_emits_instead_of_direct:
            return algebra_pb2.RelCommon(direct=algebra_pb2.RelCommon.Direct())
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        emit = algebra_pb2.RelCommon.Emit()
        field_number = 0
        for _ in symbol.output_fields:
            emit.output_mapping.append(field_number)
            field_number += 1
        return algebra_pb2.RelCommon(emit=emit)

    def visit_read_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read relation into a Substrait relation."""
        match rel.WhichOneof('read_type'):
            case 'named_table':
                result = self.visit_read_named_table_relation(rel.named_table)
            case 'data_source':
                result = self.visit_read_data_source_relation(rel.data_source)
            case _:
                raise ValueError(f'Unexpected read type: {rel.WhichOneof("read_type")}')
        result.read.common.CopyFrom(self.create_common_relation())
        return result

    def visit_filter_relation(self, rel: spark_relations_pb2.Filter) -> algebra_pb2.Rel:
        """Converts a filter relation into a Substrait relation."""
        filter_rel = algebra_pb2.FilterRel(input=self.visit_relation(rel.input))
        self.update_field_references(rel.input.common.plan_id)
        filter_rel.common.CopyFrom(self.create_common_relation())
        filter_rel.condition.CopyFrom(self.visit_expression(rel.condition))
        return algebra_pb2.Rel(filter=filter_rel)

    def visit_sort_relation(self, rel: spark_relations_pb2.Sort) -> algebra_pb2.Rel:
        """Converts a sort relation into a Substrait relation."""
        sort = algebra_pb2.SortRel(input=self.visit_relation(rel.input))
        self.update_field_references(rel.input.common.plan_id)
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
                expr=self.visit_expression(order.child),
                direction=direction))
        return algebra_pb2.Rel(sort=sort)

    def visit_limit_relation(self, rel: spark_relations_pb2.Limit) -> algebra_pb2.Rel:
        """Converts a limit relation into a Substrait FetchRel relation."""
        input_relation = self.visit_relation(rel.input)
        self.update_field_references(rel.input.common.plan_id)
        fetch = algebra_pb2.FetchRel(common=self.create_common_relation(), input=input_relation,
                                     count=rel.limit)
        return algebra_pb2.Rel(fetch=fetch)

    def visit_aggregate_relation(self, rel: spark_relations_pb2.Aggregate) -> algebra_pb2.Rel:
        """Converts an aggregate relation into a Substrait relation."""
        aggregate = algebra_pb2.AggregateRel(input=self.visit_relation(rel.input))
        self.update_field_references(rel.input.common.plan_id)
        aggregate.common.CopyFrom(self.create_common_relation())
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        for grouping in rel.grouping_expressions:
            aggregate.groupings.append(
                algebra_pb2.AggregateRel.Grouping(
                    grouping_expressions=[self.visit_expression(grouping)]))
            # TODO -- Use the same field name as what was selected in the grouping.
            symbol.generated_fields.append('grouping')
        for expr in rel.aggregate_expressions:
            aggregate.measures.append(
                algebra_pb2.AggregateRel.Measure(
                    measure=self.visit_expression_to_aggregate_function(expr))
            )
            symbol.generated_fields.append(expr.alias.name[0])
        symbol.output_fields.clear()
        symbol.output_fields.extend(symbol.generated_fields)
        return algebra_pb2.Rel(aggregate=aggregate)

    def visit_show_string_relation(self, rel: spark_relations_pb2.ShowString) -> algebra_pb2.Rel:
        """Converts a show string relation into a Substrait project relation."""
        if not self._conversion_options.implement_show_string:
            result = self.visit_relation(rel.input)
            self.update_field_references(rel.input.common.plan_id)
            return result

        # TODO -- Implement using num_rows by wrapping the input in a fetch relation.

        # TODO -- Implement what happens if truncate is not set or less than two.
        # TODO -- Implement what happens when rel.vertical is true.
        input_rel = self.visit_relation(rel.input)
        self.update_field_references(rel.input.common.plan_id)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        # TODO -- Pull the columns from symbol.input_fields.
        # TODO -- Use string_agg to aggregate all of the column input into a single string.
        # TODO -- Use a project to output a single field with the table info in it.
        symbol.output_fields.clear()
        symbol.output_fields.append('show_string')

        project = algebra_pb2.ProjectRel(input=input_rel)
        project.expressions.append(
            algebra_pb2.Expression(literal=self.visit_string_literal('hiya')))
        project.common.emit.output_mapping.append(len(symbol.input_fields))
        return algebra_pb2.Rel(project=project)

    def visit_with_columns_relation(
            self, rel: spark_relations_pb2.WithColumns) -> algebra_pb2.Rel:
        """Converts a with columns relation into a Substrait project relation."""
        input_rel = self.visit_relation(rel.input)
        project = algebra_pb2.ProjectRel(input=input_rel)
        self.update_field_references(rel.input.common.plan_id)
        symbol = self._symbol_table.get_symbol(self._current_plan_id)
        field_number = 0
        if self._conversion_options.use_project_emit_workaround:
            for _ in symbol.output_fields:
                project.expressions.append(algebra_pb2.Expression(
                    selection=algebra_pb2.Expression.FieldReference(
                        direct_reference=algebra_pb2.Expression.ReferenceSegment(
                            struct_field=algebra_pb2.Expression.ReferenceSegment.StructField(
                                field=field_number)))))
            field_number += 1
        for alias in rel.aliases:
            # TODO -- Handle the common.emit.output_mapping columns correctly.
            project.expressions.append(self.visit_expression(alias.expr))
            # TODO -- Add unique intermediate names.
            symbol.generated_fields.append('intermediate')
            symbol.output_fields.append('intermediate')
        project.common.CopyFrom(self.create_common_relation())
        if (self._conversion_options.use_project_emit_workaround or
                self._conversion_options.use_project_emit_workaround2):
            field_number = 0
            for _ in symbol.output_fields:
                project.common.emit.output_mapping.append(field_number)
                field_number += 1
            for _ in rel.aliases:
                project.common.emit.output_mapping.append(field_number)
                field_number += 1
        return algebra_pb2.Rel(project=project)

    def visit_relation(self, rel: spark_relations_pb2.Relation) -> algebra_pb2.Rel:
        """Converts a Spark relation into a Substrait one."""
        self._symbol_table.add_symbol(rel.common.plan_id, parent=self._current_plan_id,
                                      symbol_type=rel.WhichOneof('rel_type'))
        old_plan_id = self._current_plan_id
        self._current_plan_id = rel.common.plan_id
        match rel.WhichOneof('rel_type'):
            case 'read':
                result = self.visit_read_relation(rel.read)
            case 'filter':
                result = self.visit_filter_relation(rel.filter)
            case 'sort':
                result = self.visit_sort_relation(rel.sort)
            case 'limit':
                result = self.visit_limit_relation(rel.limit)
            case 'aggregate':
                result = self.visit_aggregate_relation(rel.aggregate)
            case 'show_string':
                result = self.visit_show_string_relation(rel.show_string)
            case 'with_columns':
                result = self.visit_with_columns_relation(rel.with_columns)
            case _:
                raise ValueError(f'Unexpected rel type: {rel.WhichOneof("rel_type")}')
        self._current_plan_id = old_plan_id
        return result

    def visit_plan(self, plan: plan_pb2.Plan) -> None:
        """Visits all the relations in a Substrait plan."""
        if plan.HasField('rel_root'):
            self.visit_root_relation(plan.rel_root)
