package parameter.server.algorithms.matrix.factorization

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import parameter.server.algorithms.matrix.factorization.Types.Prediction

import scala.collection.{immutable, mutable}
import scala.util.Random

class TypesTest extends FlatSpec with PropertyChecks with Matchers {

  def generatePredictions(n: Int): immutable.Seq[Prediction] = {
    for(i <- 0 until n) yield Prediction(i, Random.nextDouble())
  }

  "Inserting into TreeSet" should "give false, when IDs are equal" in {
    val oldValues = generatePredictions(10)
    val newValues = oldValues.map(x => Prediction(x.itemId, x.score + 1.0))

    val itemIdsDescendingByLength = new mutable.TreeSet[Prediction]()

    oldValues.foreach(x => itemIdsDescendingByLength.add(x))

    itemIdsDescendingByLength.size shouldBe 10

    oldValues.zip(newValues).map(x => {
      itemIdsDescendingByLength.remove(x._1)
      itemIdsDescendingByLength.add(x._2)
      itemIdsDescendingByLength.toList.map(_.itemId).distinct.size shouldBe 10
    })

  }
}
