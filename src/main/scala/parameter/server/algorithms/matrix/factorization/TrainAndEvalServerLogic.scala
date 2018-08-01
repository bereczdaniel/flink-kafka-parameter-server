package parameter.server.algorithms.matrix.factorization

import org.apache.flink.util.Collector
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationOutput
import parameter.server.communication.Messages
import parameter.server.communication.Messages.PullAnswer
import parameter.server.logic.server.ServerLogic
import parameter.server.utils.{Types, Vector}

import scala.collection.mutable

class TrainAndEvalServerLogic extends ServerLogic {
  val model = new mutable.HashMap[AnyVal, Vector]()

  override def onPullReceive(push: Messages.Pull, out: Collector[Either[Types.ParameterServerOutput, Messages.Message]]): Unit = {
    out.collect(Right(PullAnswer(push.destination, push.source, model.getOrElseUpdate(push.destination, Vector(Array.fill(10)(scala.util.Random.nextDouble()))))))
  }

  override def onPushReceive(pull: Messages.Push, out: Collector[Either[Types.ParameterServerOutput, Messages.Message]]): Unit = {
    out.collect(Left(EvaluationOutput(0,0, Types.createTopK, 0L)))
  }
}

object TrainAndEvalServerLogic {
  def apply: TrainAndEvalServerLogic = new TrainAndEvalServerLogic()
}