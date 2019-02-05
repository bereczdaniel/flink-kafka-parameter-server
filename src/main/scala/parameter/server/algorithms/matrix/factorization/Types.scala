package parameter.server.algorithms.matrix.factorization

import scala.collection.mutable

object Types {


  case class Recommendation(targetId: ItemId, topK: List[ItemId], evaluationId: Long, timestamp: Long)

  // Types needed by matrix factorization
  type UserId = Int
  type ItemId = Int
  type TopK = mutable.PriorityQueue[(ItemId, Double)]

  val topKOrdering: Ordering[(ItemId, Double)] = Ordering[Double].on[(ItemId, Double)](x => -x._2)

  def createTopK: TopK =
    new mutable.PriorityQueue[(ItemId, Double)]()(topKOrdering)
}
