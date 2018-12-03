package parameter.server.kafkaredis.logic.server

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.{Message, NotSupportedMessage, Pull, Push}
import parameter.server.utils.Types.{Parameter, ParameterServerOutput}

abstract class AsynchronousServerLogic[WK, SK, P <: Parameter] extends ServerLogic[WK, SK, P] {

  val model: ValueState[P]

  def getOrElseUpdate(newVector: P): P ={
    if(model.value() == null){
      model.update(newVector)
      newVector
    }
    else{
      model.value()
    }
  }

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


  def onPullReceive(pull: Pull[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]])
  def onPushReceive(push: Push[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]])
}
