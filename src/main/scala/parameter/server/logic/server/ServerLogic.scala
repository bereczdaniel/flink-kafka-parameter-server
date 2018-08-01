package parameter.server.logic.server

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.{Message, NotSupportedMessage, Pull, Push}
import parameter.server.utils.Types.ParameterServerOutput

abstract class ServerLogic extends RichFlatMapFunction[Message, Either[ParameterServerOutput, Message]]{
  override def flatMap(value: Message, out: Collector[Either[ParameterServerOutput, Message]]): Unit = {
    value match {
      case push: Push =>
        onPushReceive(push, out)
      case pull: Pull =>
        onPullReceive(pull, out)
      case _ =>
        throw new NotSupportedMessage
    }
  }


  def onPullReceive(push: Pull, out: Collector[Either[ParameterServerOutput, Message]])
  def onPushReceive(pull: Push, out: Collector[Either[ParameterServerOutput, Message]])
}
