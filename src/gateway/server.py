# SPDX-License-Identifier: Apache-2.0
"""SparkConnect server that drives a backend using Substrait."""
import io
import logging
from concurrent import futures
from typing import Generator

import grpc
import pyarrow
import pyspark.sql.connect.proto.base_pb2_grpc as pb2_grpc
import pyspark.sql.connect.proto.base_pb2 as pb2

from gateway.converter.conversion_options import duck_db, datafusion
from gateway.converter.spark_to_substrait import SparkSubstraitConverter
from gateway.adbc.backend import AdbcBackend


_LOGGER = logging.getLogger(__name__)


def show_string(table: pyarrow.lib.Table) -> bytes:
    """Converts a table into a byte serialized single row string column Arrow Table."""
    results_str = str(table)
    schema = pyarrow.schema([('show_string', pyarrow.string())])
    array = pyarrow.array([results_str])
    batch = pyarrow.RecordBatch.from_arrays([array], schema=schema)
    result_table = pyarrow.Table.from_batches([batch])
    buffer = io.BytesIO()
    stream = pyarrow.RecordBatchStreamWriter(buffer, schema)
    stream.write_table(result_table)
    stream.close()
    return buffer.getvalue()


def batch_to_bytes(batch: pyarrow.RecordBatch, schema: pyarrow.Schema) -> bytes:
    """Serializes a RecordBatch into a bytes."""
    result_table = pyarrow.Table.from_batches(batches=[batch])
    buffer = io.BytesIO()
    stream = pyarrow.RecordBatchStreamWriter(buffer, schema)
    stream.write_table(result_table)
    stream.close()
    return buffer.getvalue()


# pylint: disable=E1101,fixme
class SparkConnectService(pb2_grpc.SparkConnectServiceServicer):
    """Provides the SparkConnect service."""

    # pylint: disable=unused-argument
    def __init__(self, *args, **kwargs):
        # This is the central point for configuring the behavior of the service.
        self._options = duck_db()

    def ExecutePlan(
            self, request: pb2.ExecutePlanRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        _LOGGER.info('ExecutePlan: %s', request)
        convert = SparkSubstraitConverter(self._options)
        substrait = convert.convert_plan(request.plan)
        _LOGGER.debug('  as Substrait: %s', substrait)
        backend = AdbcBackend()
        results = backend.execute(substrait, self._options.backend)
        _LOGGER.debug('  results are: %s', results)

        if not self._options.implement_show_string and request.plan.root.WhichOneof(
                'rel_type') == 'show_string':
            yield pb2.ExecutePlanResponse(
                session_id=request.session_id,
                arrow_batch=pb2.ExecutePlanResponse.ArrowBatch(row_count=results.num_rows,
                                                               data=show_string(results)))
            return

        for batch in results.to_batches():
            yield pb2.ExecutePlanResponse(session_id=request.session_id,
                                          arrow_batch=pb2.ExecutePlanResponse.ArrowBatch(
                                              row_count=batch.num_rows,
                                              data=batch_to_bytes(batch, results.schema)))
        # TODO -- When spark 3.4.0 support is not required, yield a ResultComplete message here.

    def AnalyzePlan(self, request, context):
        _LOGGER.info('AnalyzePlan: %s', request)
        return pb2.AnalyzePlanResponse(session_id=request.session_id)

    def Config(self, request, context):
        _LOGGER.info('Config: %s', request)
        response = pb2.ConfigResponse(session_id=request.session_id)
        match request.operation.WhichOneof('op_type'):
            case 'set':
                for pair in request.operation.set.pairs:
                    if pair.key == 'spark-substrait-gateway.backend':
                        # Set the server backend for all connections (including ongoing ones).
                        match pair.value:
                            case 'duckdb':
                                self._options = duck_db()
                            case 'datafusion':
                                self._options = datafusion()
                            case _:
                                raise ValueError(f'Unknown backend: {pair.value}')
                response.pairs.extend(request.operation.set.pairs)
            case 'get_with_default':
                response.pairs.extend(request.operation.get_with_default.pairs)
        return response

    def AddArtifacts(self, request_iterator, context):
        _LOGGER.info('AddArtifacts')
        return pb2.AddArtifactsResponse()

    def ArtifactStatus(self, request, context):
        _LOGGER.info('ArtifactStatus')
        return pb2.ArtifactStatusesResponse()

    def Interrupt(self, request, context):
        _LOGGER.info('Interrupt')
        return pb2.InterruptResponse()

    def ReattachExecute(
            self, request: pb2.ReattachExecuteRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        _LOGGER.info('ReattachExecute')
        yield pb2.ExecutePlanResponse(
            session_id=request.session_id,
            result_complete=pb2.ExecutePlanResponse.ResultComplete())

    def ReleaseExecute(self, request, context):
        _LOGGER.info('ReleaseExecute')
        return pb2.ReleaseExecuteResponse()


def serve(port: int, wait: bool = True):
    """Starts the SparkConnect to Substrait gateway server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SparkConnectServiceServicer_to_server(SparkConnectService(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    if wait:
        server.wait_for_termination()
        return None
    return server


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, encoding='utf-8')
    serve(50051)
