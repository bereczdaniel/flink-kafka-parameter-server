package parameter.server.logic.server

import org.apache.flink.util.Collector
import parameter.server.communication.Messages.{Message, Pull, Push}
import parameter.server.utils.Types.ParameterServerOutput

trait ServerLogic {
  def onPullReceive(push: Pull, out: Collector[Either[ParameterServerOutput, Message]])
  def onPushReceive(pull: Push, out: Collector[Either[ParameterServerOutput, Message]])
}
