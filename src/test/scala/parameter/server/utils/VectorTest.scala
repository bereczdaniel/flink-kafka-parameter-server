package parameter.server.utils

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import types.Vector
import types.Vector.FactorIsNotANumberException

class VectorTest extends FlatSpec with PropertyChecks with Matchers {
  val v1 = new Vector(Array(1,2,3,4))
  val v2 = new Vector(Array(1,1,1,1))

  val negInf = new Vector(Array(0, Double.NegativeInfinity))
  val posInf = new Vector(Array(1, Double.PositiveInfinity))

  val unitVector = new Vector(Array(1,1))

  "vectorLengthSqr" should "calculate the squared length of the given vector" in {
    Vector.vectorLengthSqr(v1) shouldBe 30.0
    Vector.vectorLengthSqr(v2) shouldBe 4.0
  }

  "dotProduct" should "calculate the dot product of the vectors" in {
    Vector.dotProduct(v1,v2) shouldBe 10
  }

  "vectorSum" should "calculate the elementwise sum of two vectors" in {
    Vector.vectorSum(v1,v2).value shouldBe Array(2,3,4,5)

    Vector.vectorSum(negInf, unitVector).value shouldBe Array(1.0, Double.NegativeInfinity)
    Vector.vectorSum(posInf, unitVector).value shouldBe Array(2.0, Double.PositiveInfinity)


    assertThrows[FactorIsNotANumberException](Vector.vectorSum(posInf, negInf))
  }
}
