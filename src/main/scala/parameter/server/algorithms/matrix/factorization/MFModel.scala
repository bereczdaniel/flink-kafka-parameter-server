package parameter.server.algorithms.matrix.factorization

import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.Types.ItemId
import parameter.server.logic.worker.Model
import parameter.server.utils.Vector

class MFModel(numFactors: Int, rangeMin: Double, rangeMax: Double) extends Model[ItemId, Vector]{

  lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  /**
    * Binary operator, on how to combine two parameter
    *
    * @return The combined value of the two parameter
    */
  override def updateFunction: (Vector, Vector) => Vector = Vector.vectorSum

  override def initFunction: ItemId => Vector = seed => Vector(factorInitDesc.open().nextFactor(seed))

  /**
    * Returns the corresponding the value, if it doesn't exist, then init
    *
    * @param key
    * @return
    */
  override def get(key: ItemId): Option[Vector] = ???

  /**
    * Set the value of key to newValue. Override if there is already one.
    *
    * @param key
    * @param newValue
    */
  override def set(key: ItemId, newValue: Vector): Unit = ???
}
