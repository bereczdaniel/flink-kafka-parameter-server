package hu.sztaki.ilab.ps.common.types

import matrix.factorization.types.{ItemId, TopK, UserId, Vector}
//import org.apache.flink.streaming.api.scala._

object RecSysMessages {

  case class EvaluationRequest(userId: Int, itemId: Int, evaluationId: Long, rating: Double, ts: Long) extends WorkerInput(itemId)
  // Note: Currently unused worker input types:
  case class UserParameter(userId: Int, parameter: Vector) extends WorkerInput(userId)
  case class ItemParameter(itemId: Int, parameter: Vector) extends WorkerInput(itemId)

  /** Model output parameter type. It can be used to dump the model.
    * Note: Currently unused.
    *
    * @param id parameter id
    * @param parameter parameter vector
    */
  case class VectorModelOutput(id: AnyVal, parameter: Vector) extends ParameterServerOutput {
    override def toString: String =
      id.toString + ":" + parameter.value.tail.foldLeft(parameter.value.head.toString)((acc, c) => acc + "," + c.toString)
  }

//  def parseUserParameter(line: String): WorkerInput = {
//    val id :: params = line.split(",").toList
//    UserParameter(id.toInt, Vector(params.map(_.toDouble).toArray))
//  }
//
//  def parseItemParameter(line: String): WorkerInput = {
//    val id :: params = line.split(",").toList
//    ItemParameter(id.toInt, Vector(params.map(_.toDouble).toArray))
//  }
//
//  def readModel(env: StreamExecutionEnvironment, fileName: String, parse: String => WorkerInput): DataStream[WorkerInput] =
//    env
//      .readTextFile(fileName)
//      .map(parse)


  case class EvaluationOutput(userId: UserId, itemId: ItemId, evaluationId: Long, topK: TopK, ts: Long) extends ParameterServerOutput
}
