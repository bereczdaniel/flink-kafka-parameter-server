package parameter.server.logic.worker

import org.apache.flink.util.Collector
import parameter.server.communication.Messages.Message
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}

trait WorkerLogic {

  def onPullReceive(pullAnswer: Message, out: Collector[Either[ParameterServerOutput, Message]]): Unit

  def onInputReceive(data: WorkerInput, out: Collector[Either[ParameterServerOutput, Message]]): Unit
}
