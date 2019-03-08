package parameter.server.algorithms.matrix.factorization

import scala.collection.mutable
import parameter.server.utils.Vector

object Types {


  case class Recommendation(targetId: ItemId, topK: List[ItemId], evaluationId: Long, timestamp: Long)

  /**
    * Important properties to have:
    *   Always find (id, vector) pairs
    *   Always descending order
    * @param id
    * @param length
    */
  case class ItemVector(id: ItemId, vector: Vector) extends Ordered[ItemVector] {
    self =>
    override def compare(that: ItemVector): Int =
      if(self.vector.normSqr == that.vector.normSqr)
        self.id compare that.id
      else
        -(self.vector.normSqr compare that.vector.normSqr)
  }

  /**
    * Import properties to have:
    *   always descending on score
    * @param itemId
    * @param score
    */
  case class Prediction(itemId: ItemId, score: Double) extends Ordered[Prediction]{ self =>

    import scala.math.Ordered.orderingToOrdered

    def compare(that: Prediction): Int =
          -(self.score compare that.score)
  }

  // Types needed by matrix factorization
  type UserId = Int
  type ItemId = Int
  type TopK = mutable.PriorityQueue[Prediction]


  def createTopK: TopK =
    new mutable.PriorityQueue[Prediction]()


  def asd(): Unit ={
    val a = createTopK
  }
}
