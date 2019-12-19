package hu.sztaki.ilab.ps.kafka.logic.worker

import hu.sztaki.ilab.ps.common.types.{ParameterServerOutput, WorkerInput}
import hu.sztaki.ilab.ps.kafka.communication.Messages.Message
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import matrix.factorization.types.Parameter

abstract class WorkerLogic[WK, SK, T <: WorkerInput, P <: Parameter]
  extends RichCoFlatMapFunction[Message[SK, WK, P], T, Either[ParameterServerOutput, Message[WK, SK, P]]]{
  lazy val workerId: Int = getRuntimeContext.getIndexOfThisSubtask

  override def flatMap1(value: Message[SK, WK, P], out: Collector[Either[ParameterServerOutput, Message[WK, SK, P]]]): Unit =
    onPullReceive(value, out)

  override def flatMap2(value: T, out: Collector[Either[ParameterServerOutput, Message[WK, SK, P]]]): Unit =
    onInputReceive(value, out)

  def onPullReceive(msg: Message[SK, WK, P], out: Collector[Either[ParameterServerOutput, Message[WK, SK, P]]])

  def onInputReceive(data: T, out: Collector[Either[ParameterServerOutput, Message[WK, SK, P]]])
}
