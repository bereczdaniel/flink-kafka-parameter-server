package parameter.server.algorithms.matrix.factorization.impl.server

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector
import parameter.server.algorithms.matrix.factorization.Types.ItemId
import parameter.server.communication.Messages
import parameter.server.communication.Messages.PullAnswer
import parameter.server.logic.server.AsynchronousServerLogic
import parameter.server.utils.{Types, Vector}

class StateBackedMfServerLogic(_init: Int => Vector, _update: (Vector, Vector) => Vector)
  extends AsynchronousServerLogic[Long, Int, Vector] {
  override lazy val model: ValueState[Vector] = getRuntimeContext.getState(
    new ValueStateDescriptor[Vector]("shared parameters", classOf[Vector]))

  @transient lazy val init: Int => Vector = _init
  @transient lazy val update: (Vector, Vector) => Vector = _update

  override def onPullReceive(pull: Messages.Pull[Long, Int, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Int, Long, Vector]]]): Unit = {
    out.collect(Right(PullAnswer(pull.dest, pull.src, getOrElseUpdate(init(pull.dest)))))
  }

  override def onPushReceive(push: Messages.Push[Long, Int, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Int, Long, Vector]]]): Unit = {
    val oldParam = model.value()

    //TODO log or somehow signal the chosen path maybe?
    if(oldParam != null)
      model.update(update(oldParam, push.msg))
    else
      model.update(push.msg)
  }
}

object StateBackedMfServerLogic {
  def apply(_init: ItemId => Vector, _update: (Vector, Vector) => Vector): StateBackedMfServerLogic = new StateBackedMfServerLogic(_init, _update)
}