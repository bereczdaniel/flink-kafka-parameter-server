package hu.sztaki.ilab.ps.kafka.logic.server

import hu.sztaki.ilab.ps.common.types.ParameterServerOutput
import hu.sztaki.ilab.ps.kafka.communication.Messages.Message
import org.apache.flink.streaming.api.functions.ProcessFunction

/**
  * Represents the server node in the parameter server architecture. It provides an abstraction layer over the Flink ProcessFunction.
  * @tparam WK Type of the key on the worker nodes
  * @tparam SK Type of the key on the server nodes
  * @tparam P Type of a single parameter in the model
  */
abstract class ServerLogic[WK, SK, P]
  extends ProcessFunction[Message[WK, SK, P],  Either[ParameterServerOutput, Message[SK, WK, P]]]
