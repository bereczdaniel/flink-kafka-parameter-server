package parameter.server.logic.server

import org.apache.flink.streaming.api.functions.ProcessFunction
import parameter.server.communication.Messages.Message
import parameter.server.utils.Types.ParameterServerOutput

/**
  * Represents the server node in the parameter server architecture. It provides an abstraction layer over the Flink ProcessFunction.
  * @tparam WK Type of the key on the worker nodes
  * @tparam SK Type of the key on the server nodes
  * @tparam P Type of a single parameter in the model
  */
abstract class ServerLogic[WK, SK, P]
  extends ProcessFunction[Message[WK, SK, P],  Either[ParameterServerOutput, Message[SK, WK, P]]]
