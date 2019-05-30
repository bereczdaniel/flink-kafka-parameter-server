package parameter.server

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}

abstract class ParameterServerSkeleton[T <: WorkerInput] (val env: StreamExecutionEnvironment, val inputStream: DataStream[T]) {
  def start(): DataStream[ParameterServerOutput]
}

