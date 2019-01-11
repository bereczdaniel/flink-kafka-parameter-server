package parameter.server.utils

import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable

object Types {

  trait ParameterServerSkeleton {
    def start(): DataStream[ParameterServerOutput]
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
