package parameter.server.logic.worker

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.Message
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}

abstract class WorkerLogic[T <: WorkerInput] extends RichCoFlatMapFunction[Message, T, Either[ParameterServerOutput, Message]]{
  lazy val workerId: Int = getRuntimeContext.getIndexOfThisSubtask

  override def flatMap1(value: Message, out: Collector[Either[ParameterServerOutput, Message]]): Unit = onPullReceive(value, out)

  override def flatMap2(value: T, out: Collector[Either[ParameterServerOutput, Message]]): Unit = onInputReceive(value, out)

  def onPullReceive(pullAnswer: Message, out: Collector[Either[ParameterServerOutput, Message]]): Unit

  def onInputReceive(data: T, out: Collector[Either[ParameterServerOutput, Message]]): Unit
}
