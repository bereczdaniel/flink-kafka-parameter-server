package parameter.server

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import parameter.server.communication.Messages.Message
import parameter.server.utils.Vector
import parameter.server.utils.Types

import scala.collection.mutable.ArrayBuffer

object Utils {
  /**
    * Generate n input with the provided function
    * @param n: Number of generated instances
    * @param f: Function to instanstiate input
    * @return List of inputs
    */
  def generateInput[A](n: Int, f: Int => A): List[A] = {
    for{
      seed <- (0 until n).toList
    } yield f(seed)
  }
}

// create a testing sink
class CollectSink[T <: Message[Int, Int, Vector]] extends SinkFunction[T] {

  override def invoke(value: T): Unit = {
    synchronized {
      CollectSink.values += value
    }
  }
}

object CollectSink {

  // must be static
  val values: ArrayBuffer[Message[Int, Int, Vector]] = new ArrayBuffer[Message[Int, Int, Vector]]()
}

// create a testing sink
class CollectOutputSink[T <: Types.ParameterServerOutput] extends SinkFunction[T] {

  override def invoke(value: T): Unit = {
    synchronized {
      CollectOutputSink.values += value
    }
  }
}

object CollectOutputSink {

  // must be static
  val values: ArrayBuffer[Types.ParameterServerOutput] = new ArrayBuffer[Types.ParameterServerOutput]()
}