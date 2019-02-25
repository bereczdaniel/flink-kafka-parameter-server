package parameter.server.algorithms.matrix.factorization.impl

import parameter.server.algorithms.factors.SGDUpdater
import parameter.server.algorithms.matrix.factorization.Types.{ItemId, TopK}
import parameter.server.algorithms.models.Model
import parameter.server.algorithms.pruning.LEMPPruningStrategy
import parameter.server.utils.Vector

import scala.util.Random

class ItemModel(learningRate: Double, negativeSampleRate: Int, numFactors: Int,
                rangeMin: Double, rangeMax: Double, bucketSize: Int,
                K: Int, pruningStrategy: LEMPPruningStrategy) extends Model[ItemId, Vector, TopK] {

  override val model: LEMP = new LEMP(numFactors, rangeMin, rangeMax, bucketSize, K, pruningStrategy)

  lazy val SGDUpdater = new SGDUpdater(learningRate)

  def predict(userVector: Vector): TopK = {

    model.generateTopK(userVector)
  }


  /**
    * Update the model, using the given user-item pair
    * @param userVector
    * @param itemId
    * @param rating
    * @return
    */
  def train(userVector: Vector, itemId: ItemId, rating: Double): Vector = {
    val negativeUserDelta = calculateNegativeSamples(Some(itemId), userVector)
    val itemVector = model.getOrElseInit(itemId)

    val (positiveUserDelta, itemDelta) = SGDUpdater.delta(rating, userVector.value, itemVector.value)

    model.updateWith(itemId, Vector(itemDelta))
    Vector.vectorSum(negativeUserDelta, Vector(positiveUserDelta))
  }


  /**
    * Set a model parameter
    * @param itemId
    * @param param
    */
  def set(itemId: ItemId, param: Vector): Unit =
    model.set(itemId, param)

  //TODO check performance of conversion between Array[Double] and Vector
  //TODO tests
  def calculateNegativeSamples(itemId: Option[ItemId], userVector: Vector): Vector = {
    val possibleNegativeItems =
      itemId match {
        case Some(id) => model.keys.filterNot(_ == id)
        case None     => model.keys
      }

    (0 until  math.min(negativeSampleRate, possibleNegativeItems.length))
      .foldLeft(Vector(numFactors))((vector, _) => {
        val negItemId = possibleNegativeItems(Random.nextInt(possibleNegativeItems.length))
        val negItemVector = model(negItemId)

        val (userDelta, itemDelta) = SGDUpdater.delta(0.0, userVector.value, negItemVector.value)
        model.updateWith(negItemId, Vector(itemDelta))
        Vector.vectorSum(Vector(userDelta), vector)
      })
  }
}
