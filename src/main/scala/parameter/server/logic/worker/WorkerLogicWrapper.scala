package parameter.server.logic.worker

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.Message
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}

class WorkerLogicWrapper(workerLogic: WorkerLogic) extends RichCoFlatMapFunction[Message, WorkerInput, Either[ParameterServerOutput, Message]]{

  override def flatMap1(value: Message, out: Collector[Either[ParameterServerOutput, Message]]): Unit =
    workerLogic.onPullReceive(value, out)

  override def flatMap2(value: WorkerInput, out: Collector[Either[ParameterServerOutput, Message]]): Unit =
    workerLogic.onInputReceive(value, out)
}
