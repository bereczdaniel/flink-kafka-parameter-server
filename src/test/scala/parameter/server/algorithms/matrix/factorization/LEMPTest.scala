package parameter.server.algorithms.matrix.factorization

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import parameter.server.algorithms.matrix.factorization.Types.ItemVector
import parameter.server.algorithms.matrix.factorization.impl.LEMP
import parameter.server.algorithms.pruning.{LEMPPruningStrategy, LI}
import parameter.server.utils.Vector

import scala.collection.mutable
import scala.util.Random

class LEMPTest extends FlatSpec with PropertyChecks with Matchers with PrivateMethodTester {

  lazy val numFactorsDefault = 10
  lazy val rangeMinDefault : Double = -0.01
  lazy val rangeMaxDefault : Double = 0.01
  lazy val bucketSizeDefault = 100
  lazy val KDefault  = 5
  lazy val pruningStrategyDefault  = LI(5, 2.5)

  def generateRandomNumbers(n: Int): Iterable[Int] =
    for(_ <- 0 to n) yield Random.nextInt(n*100)

  lazy val lempDefault = new LEMP(numFactorsDefault, rangeMinDefault, rangeMaxDefault, bucketSizeDefault, KDefault, pruningStrategyDefault)

  val getItemIdsDescendingByLength = PrivateMethod[mutable.TreeSet[Types.ItemVector]]('getItemIdsDescendingByLength)

//  class LEMPTesting(numFactors: Int, rangeMin: Double, rangeMax: Double, bucketSize: Int,
//           K: Int, pruningStrategy: LEMPPruningStrategy) extends LEMP(numFactors, rangeMin, rangeMax, bucketSize, K, pruningStrategy) with PrivateMethodTester


  "initFunction" should "generate the same vector for the same id within the given range" in {
    for{
      numFactors <- List(5, 10, 20)
      rangeMin <- List(-1, -0.01, 0, 0.01)
      rangeMax <- List(0.01, 2)
    }{
      val lemp = new LEMP(numFactors, rangeMin, rangeMax, bucketSizeDefault, KDefault, pruningStrategyDefault)
      (0 until 100).foreach(i => {
        val vector = lemp.initFunction(i)
        vector.value.length shouldBe numFactors
        vector.value.forall(x => x >= rangeMin && x <= rangeMax) shouldBe true

        val second = lemp.initFunction(i)
        second shouldBe vector
      })
    }
  }

  "set" should "add new item to LEMP" in {
    val lemp = new LEMP(numFactorsDefault, rangeMaxDefault, rangeMaxDefault, bucketSizeDefault, KDefault, pruningStrategyDefault)

    lemp.set(0, Vector(Array(1.1)))

    lemp.get(0).get shouldBe Vector(Array(1.1))
  }

  "updateItemIdsByLength" should "Remove the previous vector for the given id, and add the new one" in {
    val lemp = new LEMP(numFactorsDefault, rangeMaxDefault, rangeMaxDefault, bucketSizeDefault, KDefault, pruningStrategyDefault)
    lemp.set(0, Vector(Array(1.1)))


    val itemIdsDescendingByLength = lemp invokePrivate getItemIdsDescendingByLength()
    itemIdsDescendingByLength.size shouldBe 1
    itemIdsDescendingByLength.head shouldBe ItemVector(0, Vector(Array(1.1)))

    val updateItemIdsByLength = PrivateMethod[Unit]('updateItemIdsByLength)
    lemp invokePrivate updateItemIdsByLength(0, Vector(Array(2.2)), Vector(Array(1.1)))
    itemIdsDescendingByLength.size shouldBe 1
    itemIdsDescendingByLength.head shouldBe ItemVector(0, Vector(Array(2.2)))
  }

  "updateWith" should "change the old item" in {
    val lemp = new LEMP(numFactorsDefault, rangeMaxDefault, rangeMaxDefault, bucketSizeDefault, KDefault, pruningStrategyDefault)

    lemp.set(0, Vector(Array(1.0)))
    val itemIdsDescendingByLength = lemp invokePrivate getItemIdsDescendingByLength()
    itemIdsDescendingByLength.size shouldBe 1
    itemIdsDescendingByLength.head shouldBe ItemVector(0, Vector(Array(1.0)))

    lemp.updateWith(0, Vector(Array(2.0)))
    lemp.get(0).get shouldBe Vector(Array(3.0))
    itemIdsDescendingByLength.size shouldBe 1
    itemIdsDescendingByLength.head shouldBe ItemVector(0, Vector(Array(3.0)))

    lemp.keys.size shouldBe 1
    lemp.keys.head shouldBe 0
  }

  "getOrElseInit" should "Returns the vector for the given id, or generates a new one if there is none" in {
    val lemp = new LEMP(numFactorsDefault, rangeMaxDefault, rangeMaxDefault, bucketSizeDefault, KDefault, pruningStrategyDefault)

    lemp.get(0) shouldBe None

    val initVect = lemp.getOrElseInit(0)
    initVect.value.size shouldBe 10

    lemp.getOrElseInit(0) shouldBe initVect

    lemp.keys.size shouldBe 1
    lemp.keys.head shouldBe 0
  }

//  "update ids by length" should "stay the size of the array the same" in {
//
//  }

  "generate top K" should "give back the top k most similar vectors" in {

    val x = (1 to 100) map ( _ => {
      val lemp = new LEMP(numFactorsDefault, rangeMinDefault, rangeMaxDefault, bucketSizeDefault, KDefault, pruningStrategyDefault)
      val query = lemp.initFunction(Random.nextInt())
      val vectors: List[(Vector, Int)] = generateRandomNumbers(100).map(s => (lemp.initFunction(s), s)).toList

      vectors.foreach(v => lemp.set(v._2, v._1))

      val topK = vectors.map(x => (Vector.dotProduct(x._1, query), x._2)).sortBy(-_._1).take(KDefault)
      val results = lemp.generateTopK(query).toList.sortBy(-_.score)

      results.map(_.itemId) == topK.map(_._2)
    })

    x.forall(p => p) shouldBe true
  }

}