package parameter.server

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import parameter.server.communication.Messages.Message
import parameter.server.utils.Vector

import scala.collection.mutable.ArrayBuffer

class Utils {

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