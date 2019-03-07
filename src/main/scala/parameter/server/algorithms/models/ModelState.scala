package parameter.server.algorithms.models

/**
  * Represents a machine learning model.
  * @tparam K: Type of the key
  * @tparam P: Type of the parameter
  */
abstract class ModelState[K,P]() {

  def apply(key: K): P = get(key).get

  /**
    * Binary operator, on how to combine two parameter
    * @return The combined value of the two parameter
    */
  def updateFunction: (P,P) => P

  /**
    * Defines how an element should be initialized
    * @return
    */
  def initFunction: K => P

  def keys: Array[K]

  /**
    * Returns the corresponding value, if it doesn't exist, then None
    * @param key
    * @return
    */
  def get(key: K): Option[P]

  /**
    * Set the value of key to newValue. Override if there is already one.
    * @param key
    * @param newValue
    */
  def set(key: K, newValue: P): Unit

  /**
    * Update the existing value with the newValue using the updateFunction. If there is no original value, one will be
    * initialized and used as original.
    * @param key
    * @param newValue
    */
  def updateWith(key: K, newValue: P): Unit =
    set(key, updateFunction(getOrElseInit(key), newValue))

  /**
    * Get the value of the key, If there is no corresponding value, initialize it.
    * @param key
    * @return
    */
  def getOrElseInit(key: K): P =
    get(key) match {
      case Some(orig) => orig
      case None =>
        val newValue = initFunction(key)
        set(key, newValue)
        newValue
    }
}
