package hu.sztaki.ilab.ps.kafka

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}

abstract class ParameterServerSkeleton[T <: WorkerInput] (val env: StreamExecutionEnvironment, val inputStream: DataStream[T]) {
  //TODO rename : not start but build up the job graph
  def start(): DataStream[ParameterServerOutput]
}

