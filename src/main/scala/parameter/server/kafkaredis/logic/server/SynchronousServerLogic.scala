package parameter.server.kafkaredis.logic.server

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.{Message, NotSupportedMessage, Pull, Push}
import parameter.server.utils.Types.{Parameter, ParameterServerOutput}

abstract class SynchronousServerLogic[WK, SK, P <: Parameter]
  extends ServerLogic[WK, SK, P] {

  override def processElement(value: Message[WK, SK, P],
                              ctx: ProcessFunction[Message[WK, SK, P], Either[ParameterServerOutput, Message[SK, WK, P]]]#Context,
                              out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]]): Unit = {
    value match {
      case push: Push[WK, SK, P] =>
        onPushReceive(push, out)
        activeUpdate = false
      case pull: Pull[WK, SK, P] =>
        if(activeUpdate) {
          unansweredPulls.add(pull)
          ctx.timerService().registerProcessingTimeTimer(5000)
        }
        else{
          activeUpdate = true
          onPullReceive(pull, out)
        }
      case _ =>
        throw new NotSupportedMessage
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: ProcessFunction[Message[WK, SK, P], Either[ParameterServerOutput, Message[SK, WK, P]]]#OnTimerContext,
                       out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]]): Unit = {
    println("onTimer")
    if(activeUpdate)
      ctx.timerService().registerProcessingTimeTimer(5000)
    else{
      activeUpdate=true
      onPullReceive(unansweredPulls.get().iterator().next(), out)
    }
  }

  var activeUpdate: Boolean = false

  val model: ValueState[P]
  lazy val unansweredPulls: ListState[Pull[WK, SK, P]] =
    getRuntimeContext
      .getListState(new ListStateDescriptor[Pull[WK, SK, P]]("unanswered messages", classOf[Pull[WK, SK, P]]))

  def getOrElseUpdate(newVector: P): P ={
    if(model.value() == null){
      model.update(newVector)
      newVector
    }
    else{
      model.value()
    }
  }

  def onPullReceive(pull: Pull[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]])
  def onPushReceive(push: Push[WK, SK, P], out: Collector[Either[ParameterServerOutput, Message[SK, WK, P]]])

}
