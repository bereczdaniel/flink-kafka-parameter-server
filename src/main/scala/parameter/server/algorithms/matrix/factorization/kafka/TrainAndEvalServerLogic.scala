package parameter.server.algorithms.matrix.factorization.kafka

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector
import parameter.server.communication.Messages
import parameter.server.communication.Messages.PullAnswer
import parameter.server.kafka.logic.server.SynchronousServerLogic
import parameter.server.utils.Types.ItemId
import parameter.server.utils.{Types, Vector}

class TrainAndEvalServerLogic(_init: Int => Vector, _update: (Vector, Vector) => Vector)
  extends SynchronousServerLogic[Long, Int, Vector] {
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

    model.update(update(oldParam, push.msg))
  }
}

object TrainAndEvalServerLogic {
  def apply(_init: ItemId => Vector, _update: (Vector, Vector) => Vector): TrainAndEvalServerLogic = new TrainAndEvalServerLogic(_init, _update)
}