package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.server

import hu.sztaki.ilab.ps.common.types.ParameterServerOutput
import hu.sztaki.ilab.ps.kafka.communication.Messages
import hu.sztaki.ilab.ps.kafka.communication.Messages.PullAnswer
import hu.sztaki.ilab.ps.kafka.logic.server.AsynchronousServerLogic
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector
import matrix.factorization.types.{ItemId, Vector}

class StateBackedMfServerLogic(_init: Int => Vector, _update: (Vector, Vector) => Vector)
  extends AsynchronousServerLogic[Long, Int, Vector] {
  override lazy val model: ValueState[Vector] = getRuntimeContext.getState(
    new ValueStateDescriptor[Vector]("shared parameters", classOf[Vector]))

  @transient lazy val init: Int => Vector = _init
  @transient lazy val update: (Vector, Vector) => Vector = _update

  override def onPullReceive(pull: Messages.Pull[Long, Int, Vector],
                             out: Collector[Either[ParameterServerOutput, Messages.Message[Int, Long, Vector]]]): Unit = {
    out.collect(Right(PullAnswer(pull.dest, pull.src, getOrElseUpdate(init(pull.dest)))))
  }

  override def onPushReceive(push: Messages.Push[Long, Int, Vector],
                             out: Collector[Either[ParameterServerOutput, Messages.Message[Int, Long, Vector]]]): Unit = {
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