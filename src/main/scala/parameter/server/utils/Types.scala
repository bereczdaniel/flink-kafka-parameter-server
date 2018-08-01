package parameter.server.utils

import scala.collection.mutable

object Types {

  // Represents an arbitrary element of a machine learning model (e.g. weight of feature)
  type Parameter = Any
  type ParameterUpdate = Any

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
