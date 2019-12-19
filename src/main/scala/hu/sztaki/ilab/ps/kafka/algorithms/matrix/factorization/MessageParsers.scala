package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization

import hu.sztaki.ilab.ps.kafka.communication.Messages.{Message, NotSupportedMessage, Pull, PullAnswer, Push}
import matrix.factorization.types.Vector

object MessageParsers {
  
  /** Parse a pull(req) or push message from a string
    *
    * @return
    */
  def pullOrPushFromString(line: String): Message[Long, Int, Vector] = {
    val fields = line.split(":")

    fields.head match {
      case "Pull" => Pull(fields(1).toLong, fields(2).toInt)
      case "Push" => Push(fields(1).toLong, fields(2).toInt, Vector(fields(3).split(",").map(_.toDouble)))
      case _ =>
        throw new NotSupportedMessage
        null
    }
  }

  /** Parse a pull answer from a string
    *
    * @return
    */
  def pullAnswerFromString(line: String): PullAnswer[Long, Int, Vector] = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toLong, Vector(fields(2).split(",").map(_.toDouble)))
  }

}
