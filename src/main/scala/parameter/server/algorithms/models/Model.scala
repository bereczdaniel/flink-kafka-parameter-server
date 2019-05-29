package parameter.server.algorithms.models

abstract class Model[K, P, T] {

  protected val model: ModelState[K,P]

}
