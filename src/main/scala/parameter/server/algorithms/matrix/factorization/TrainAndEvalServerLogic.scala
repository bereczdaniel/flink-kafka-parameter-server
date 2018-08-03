package parameter.server.algorithms.matrix.factorization

import org.apache.flink.util.Collector
import parameter.server.communication.Messages
import parameter.server.communication.Messages.PullAnswer
import parameter.server.logic.server.ServerLogic
import parameter.server.utils.Types.ItemId
import parameter.server.utils.{Types, Vector}

import scala.collection.mutable

class TrainAndEvalServerLogic(_init: Int => Vector, _update: (Vector, Vector) => Vector) extends ServerLogic[Int, Int, Vector] {
  val model = new mutable.HashMap[Int, Vector]()

  @transient lazy val init: Int => Vector = _init
  @transient lazy val update: (Vector, Vector) => Vector = _update

  override def onPullReceive(pull: Messages.Pull[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Int, Int, Vector]]]): Unit = {
    out.collect(Right(PullAnswer(pull.dest, pull.src, model.getOrElseUpdate(pull.dest, init(pull.dest)))))
  }

  override def onPushReceive(push: Messages.Push[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Int, Int, Vector]]]): Unit = {
    val oldParam = model(push.destination)

    model.update(push.destination, update(oldParam, push.msg))
  }
}

object TrainAndEvalServerLogic {
  def apply(_init: ItemId => Vector, _update: (Vector, Vector) => Vector): TrainAndEvalServerLogic = new TrainAndEvalServerLogic(_init, _update)
}