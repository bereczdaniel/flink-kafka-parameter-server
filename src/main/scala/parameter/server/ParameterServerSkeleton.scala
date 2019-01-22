package parameter.server

import org.apache.flink.streaming.api.scala.DataStream
import parameter.server.communication.Messages._
import parameter.server.utils.Types.ParameterServerOutput
import parameter.server.utils.Vector

trait ParameterServerSkeleton {
  def start(): DataStream[ParameterServerOutput]
}

object ParameterServerSkeleton {
  /** Util function to kafka communication
    *
    * @return
    */
  def workerToServerParse(line: String): Message[Long, Int, Vector] = {
    val fields = line.split(":")

    fields.head match {
      case "Pull" => Pull(fields(1).toLong, fields(2).toInt)
      case "Push" => Push(fields(1).toLong, fields(2).toInt, Vector(fields(3).split(",").map(_.toDouble)))
      case _ =>
        throw new NotSupportedMessage
        null
    }
  }

  /** Util function to create PS
    *
    * @return
    */
  def pullAnswerFromString(line: String): PullAnswer[Long, Int, Vector] = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toLong, Vector(fields(2).split(",").map(_.toDouble)))
  }

}
