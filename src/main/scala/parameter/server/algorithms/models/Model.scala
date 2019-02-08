package parameter.server.algorithms.models

abstract class Model[K, P, T] {

  val model: ModelState[K,P]

}
