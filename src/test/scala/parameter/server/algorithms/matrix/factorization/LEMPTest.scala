package parameter.server.algorithms.matrix.factorization

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import parameter.server.algorithms.matrix.factorization.impl.LEMP
import parameter.server.algorithms.pruning.LI
import parameter.server.utils.Vector

import scala.util.Random

class LEMPTest extends FlatSpec with PropertyChecks with Matchers {

  lazy val numFactorsDefault = 10
  lazy val rangeMinDefault : Double = -0.01
  lazy val rangeMaxDefault : Double = 0.01
  lazy val bucketSizeDefault = 100
  lazy val KDefault  = 5
  lazy val pruningStrategyDefault  = LI(5, 2.5)

  def generateRandomNumbers(n: Int): Iterable[Int] =
    for(_ <- 0 to n) yield Random.nextInt(n*100)

  lazy val lempDefault = new LEMP(numFactorsDefault, rangeMinDefault, rangeMaxDefault, bucketSizeDefault, KDefault, pruningStrategyDefault)


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
        vector.value.forall(x => x >= rangeMin && x <= rangeMax)

        val second = lemp.initFunction(i)
        second shouldBe vector
      })
    }
  }

  "update ids by length" should "stay the size of the array the same" in {

  }

  "set" should "add new item to LEMP" in {
    val lemp = new LEMP(numFactorsDefault, rangeMaxDefault, rangeMaxDefault, bucketSizeDefault, KDefault, pruningStrategyDefault)

    lemp.set(0, Vector(Array(1.1)))

    lemp.get(0).get shouldBe Vector(Array(1.1))
  }

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

    x shouldBe (1 to 100).map(_ =>true)
  }

}