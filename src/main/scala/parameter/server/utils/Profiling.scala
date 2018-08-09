package parameter.server.utils

object Profiling {

  def time[R](name: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(name + "," + (t1 - t0)*0.000001)
    result
  }
}
