package parameter.server.utils

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import parameter.server.algorithms.Metrics

class MetricsTest extends FlatSpec with PropertyChecks with Matchers{

  val Eps = 1e-3

  "IDCG" should "return the maximal potential score" in {
  val targets = List(1,2,3,4,5)

  Metrics.IDCG(targets, 10) shouldBe (2.948 +- Eps)

  Metrics.IDCG(targets, 2) shouldBe (1.63 +- Eps)
}

  "DCG" should "return the discounted cumulative gain" in {
  val targets = List(1,2,3,4,5)
  var topK = List(2,4,6,8,10)

  Metrics.dcg(topK, targets) shouldBe (1.63 +- Eps)

  topK = List(6,7,8,9,10)

  Metrics.dcg(topK, targets) shouldBe 0.0

  topK = List(0,2,3,6,4)

  Metrics.dcg(topK, targets) shouldBe (1.517 +- Eps)
}

  "nDCG" should "return the normalized discounted cumulative gain" in {
  val targets = List(1,2,3,4,5)
  var topK = List(2,4,6,8,10)

  Metrics.nDCG(topK, targets) shouldBe (0.553 +- Eps)

  topK = List(6,7,8,9,10)

  Metrics.nDCG(topK, targets) shouldBe 0.0

  topK = List(0,2,3,6,4)

  Metrics.nDCG(topK, targets) shouldBe (0.515 +- Eps)
}
}
