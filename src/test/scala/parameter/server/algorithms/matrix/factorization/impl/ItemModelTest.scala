package parameter.server.algorithms.matrix.factorization.impl

import org.scalatest.{FlatSpec, Matchers}
import parameter.server.algorithms.pruning.LI
import parameter.server.utils.Vector

class ItemModelTest extends FlatSpec with Matchers {

  lazy val learningRate = 1.0
  lazy val negativeSampleRate = 1
  lazy val numFactorsDefault = 10
  lazy val rangeMinDefault : Double = -0.01
  lazy val rangeMaxDefault : Double = 0.01
  lazy val bucketSizeDefault = 10
  lazy val KDefault  = 20
  lazy val pruningStrategyDefault  = LI(5, 2.5)

  "calculateNegativeSamples" should "not use the current id of the item vector to generate negative samples" in {
    val itemModel = new ItemModel(1.0, 2, 10, rangeMinDefault, rangeMaxDefault,
      bucketSizeDefault, KDefault, pruningStrategyDefault)
    itemModel.set(0, Vector(Array.fill(numFactorsDefault)(2.0)))
    itemModel.set(1, Vector(Array.fill(numFactorsDefault)(1.0)))

    // should cahanged only the vector with id 1
    itemModel.calculateNegativeSamples(Some(0), Vector(Array.fill(numFactorsDefault)(1.0))) shouldBe Vector(Array.fill(numFactorsDefault)(-10.0))
    // this vector should be updated by Vector(Array.fill(numFactorsDefault)(-10.0))
    itemModel.model.get(1).get shouldBe Vector(Array.fill(numFactorsDefault)(-9.0))
    // this vector should be remain the same
    itemModel.model.get(0).get shouldBe Vector(Array.fill(numFactorsDefault)(2.0))

    // should cahanged only the vector with id 0
    itemModel.calculateNegativeSamples(Some(1), Vector(Array.fill(numFactorsDefault)(1.0))) shouldBe Vector(Array.fill(numFactorsDefault)(-40.0))
    // this vector should be remain the same
    itemModel.model.get(1).get shouldBe Vector(Array.fill(numFactorsDefault)(-9.0))
    // this vector should be updated by Vector(Array.fill(numFactorsDefault)(-20.0))
    itemModel.model.get(0).get shouldBe Vector(Array.fill(numFactorsDefault)(-18.0))
  }

  "calculateNegativeSamples" should "combinate negative samples for user delta and change the involved items vectors and choose one id only once for sampling" in {
    val itemModel = new ItemModel(1.0, 2, 10, rangeMinDefault, rangeMaxDefault,
      bucketSizeDefault, KDefault, pruningStrategyDefault)
    itemModel.set(0, Vector(Array.fill(numFactorsDefault)(1.0)))
    itemModel.set(1, Vector(Array.fill(numFactorsDefault)(1.0)))
    itemModel.set(2, Vector(Array.fill(numFactorsDefault)(0.0)))

    // should cahanged only the vector with id 0 and 1
    itemModel.calculateNegativeSamples(Some(2), Vector(Array.fill(numFactorsDefault)(1.0))) shouldBe Vector(Array.fill(numFactorsDefault)(-20.0))
    // this vector should be remain the same
    itemModel.model.get(2).get shouldBe Vector(Array.fill(numFactorsDefault)(0.0))
    // this vector should be updated by Vector(Array.fill(numFactorsDefault)(-10.0))
    itemModel.model.get(0).get should not be Vector(Array.fill(numFactorsDefault)(1.0))
    itemModel.model.get(1).get should not be Vector(Array.fill(numFactorsDefault)(1.0))
  }



}
