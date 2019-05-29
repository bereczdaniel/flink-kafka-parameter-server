package parameter.server.algorithms.matrix.factorization

import org.apache.flink.streaming.api.scala._
import parameter.server.algorithms.matrix.factorization.Types.{ItemId, Prediction, UserId}
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}
import parameter.server.utils.Vector

object RecSysMessages {

  case class Rating(userId: UserId, itemId: ItemId, rating: Double) extends WorkerInput(itemId)

  case class EvaluationRequest(userId: Int, itemId: Int, evaluationId: Long, rating: Double, ts: Long) extends WorkerInput(itemId)
  case class UserParameter(userId: Int, parameter: Vector) extends WorkerInput(userId)
  case class ItemParameter(itemId: Int, parameter: Vector) extends WorkerInput(itemId)
  case class NegativeSample(userId: UserId, itemId: ItemId, rating: Double) extends WorkerInput

  case class VectorModelOutput(id: AnyVal, parameter: Vector) extends ParameterServerOutput {
    override def toString: String =
      id.toString + ":" + parameter.value.tail.foldLeft(parameter.value.head.toString)((acc, c) => acc + "," + c.toString)
  }

  def parseUserParameter(line: String): WorkerInput = {
    val id :: params = line.split(",").toList
    UserParameter(id.toInt, Vector(params.map(_.toDouble).toArray))
  }

  def parseItemParameter(line: String): WorkerInput = {
    val id :: params = line.split(",").toList
    ItemParameter(id.toInt, Vector(params.map(_.toDouble).toArray))
  }

  def readModel(env: StreamExecutionEnvironment, fileName: String, parse: String => WorkerInput): DataStream[WorkerInput] =
    env
      .readTextFile(fileName)
      .map(parse)


  case class EvaluationOutput(itemId: Int, evaluationId: Long, topK: List[Prediction], ts: Long) extends ParameterServerOutput
}
