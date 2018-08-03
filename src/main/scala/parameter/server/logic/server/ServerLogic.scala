package parameter.server.logic.server

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.{Message, NotSupportedMessage, Pull, Push}
import parameter.server.utils.Types.{Parameter, ParameterServerOutput}

abstract class ServerLogic[WK, SK, P <: Parameter] extends RichFlatMapFunction[Message[WK, SK, P], Either[ParameterServerOutput, Message[SK, WK, P]]]{
  override def flatMap(value: Message[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]]): Unit = {
    value match {
      case push: Push[WK, SK, P] =>
        onPushReceive(push, out)
      case pull: Pull[WK, SK, P] =>
        onPullReceive(pull, out)
      case _ =>
        throw new NotSupportedMessage
    }
  }


  def onPullReceive(pull: Pull[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]])
  def onPushReceive(push: Push[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]])
}
