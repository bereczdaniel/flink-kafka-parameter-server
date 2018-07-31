package parameter.server.utils

object IDGenerator {
  private val n = new java.util.concurrent.atomic.AtomicLong

  /**
    * Generates a random rating ID
    */
  def next: Long = n.getAndIncrement()
}
