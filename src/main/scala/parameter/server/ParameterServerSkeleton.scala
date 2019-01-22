package parameter.server

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import parameter.server.communication.Messages._
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}
import parameter.server.utils.Vector

abstract class ParameterServerSkeleton[T <: WorkerInput] (val env: StreamExecutionEnvironment, val inputStream: DataStream[T]) {
  def start(): DataStream[ParameterServerOutput]
}

