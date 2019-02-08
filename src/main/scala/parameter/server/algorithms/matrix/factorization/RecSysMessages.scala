package parameter.server.algorithms.matrix.factorization

import parameter.server.algorithms.matrix.factorization.Types.{ItemId, Prediction, UserId}
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}
import parameter.server.utils.Vector

object RecSysMessages {

  case class Rating(userId: UserId, itemId: ItemId, rating: Double) extends WorkerInput(userId)
  case class EvaluationRequest(userId: Int, itemId: Int, evaluationId: Long, rating: Double, ts: Long) extends WorkerInput(userId)
  case class NegativeSample(userId: UserId, itemId: ItemId, rating: Double) extends WorkerInput

  case class VectorModelOutput(id: AnyVal, parameter: Vector) extends ParameterServerOutput {
    override def toString: String =
      id.toString + ":" + parameter.value.tail.foldLeft(parameter.value.head.toString)((acc, c) => acc + "," + c.toString)
  }

  case class EvaluationOutput(itemId: Int, evaluationId: Long, topK: List[Prediction], ts: Long) extends ParameterServerOutput
}
