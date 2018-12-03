package parameter.server.kafkaredis.logic.server

import org.apache.flink.streaming.api.functions.ProcessFunction
import parameter.server.communication.Messages.Message
import parameter.server.utils.Types.ParameterServerOutput

abstract class ServerLogic[WK, SK, P]
  extends ProcessFunction[Message[WK, SK, P],  Either[ParameterServerOutput, Message[SK, WK, P]]]
