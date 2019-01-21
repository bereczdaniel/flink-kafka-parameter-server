package parameter.server.utils

import org.apache.flink.streaming.api.scala.DataStream
import parameter.server.communication.Messages._

import scala.collection.mutable

object Types {

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

  case class Recommendation(targetId: ItemId, topK: List[ItemId], evaluationId: Long, timestamp: Long)

  // Represents an arbitrary element of a machine learning model (e.g. weight of feature)
  trait Parameter

  // Represents the input of the parameter server
  abstract class WorkerInput(val destination: AnyVal)

  trait ParameterServerOutput

  // Types needed by matrix factorization
  type UserId = AnyVal
  type ItemId = AnyVal
  type TopK = mutable.PriorityQueue[(ItemId, Double)]

  val topKOrdering: Ordering[(ItemId, Double)] = Ordering[Double].on[(ItemId, Double)](x => -x._2)

  def createTopK: TopK =
    new mutable.PriorityQueue[(ItemId, Double)]()(topKOrdering)
}
