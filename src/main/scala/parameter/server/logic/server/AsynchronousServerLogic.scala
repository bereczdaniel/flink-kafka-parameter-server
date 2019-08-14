package parameter.server.logic.server

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.{Message, NotSupportedMessage, Pull, Push}
import parameter.server.utils.Types.ParameterServerOutput
import matrix.factorization.types.Parameter

/**
  * Server logic using asynchronous parameter update.
  * @tparam WK Type of the key on the worker nodes
  * @tparam SK Type of the key on the server nodes
  * @tparam P Type of a single parameter in the model
  */
abstract class AsynchronousServerLogic[WK, SK, P <: Parameter] extends ServerLogic[WK, SK, P] {

  /**
    * Part of the shared model stored on a given node
    */
  val model: ValueState[P]

  /**
    * Logic of how to process a pull message
    * @param pull The message
    * @param out Collector for emitting elements in the downstream
    */
  def onPullReceive(pull: Pull[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]])

  /**
    * Logic of how to process a push message
    * @param push The message
    * @param out Collector for emitting elements in the downstream
    */
  def onPushReceive(push: Push[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]])

  /**
    * Asynchronous processing of the incoming push and pull messages
    * @param value Incoming message
    * @param ctx Context of the Flink ProcessFunction
    * @param out Collector for emitting elements in the downstream
    */
  override def processElement(value: Message[WK, SK, P],
                              ctx: ProcessFunction[Message[WK, SK, P], Either[ParameterServerOutput, Message[SK, WK, P]]]#Context,
                              out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]]): Unit = {
    value match {
      case push: Push[WK, SK, P] =>
        onPushReceive(push, out)
      case pull: Pull[WK, SK, P] =>
        onPullReceive(pull, out)
      case _ =>
        throw new NotSupportedMessage
    }
  }

  /**
    * Utility function for getting a model parameter
    * @param newVector Initial parameter, if key not yet had been initialized
    * @return Parameter for the currently active key
    */
  def getOrElseUpdate(newVector: P): P ={
    if(model.value() == null){
      model.update(newVector)
      newVector
    }
    else{
      model.value()
    }
  }
}
