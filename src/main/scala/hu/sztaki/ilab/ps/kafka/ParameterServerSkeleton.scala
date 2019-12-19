package hu.sztaki.ilab.ps.kafka

import hu.sztaki.ilab.ps.common.types.{ParameterServerOutput, WorkerInput}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

abstract class ParameterServerSkeleton[T <: WorkerInput] (val env: StreamExecutionEnvironment, val inputStream: DataStream[T]) {
  def buildJobGraph(): DataStream[ParameterServerOutput]
}

