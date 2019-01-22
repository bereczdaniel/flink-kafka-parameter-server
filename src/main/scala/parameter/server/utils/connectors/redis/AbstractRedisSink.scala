package parameter.server.utils.connectors.redis

import com.redis.RedisClient
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

abstract class AbstractRedisSink[T](host: String, port: Integer) extends RichSinkFunction[T] {

  lazy val redisClient = new RedisClient(host, port)

  override def invoke(msg: T) = {
    processMessage(msg)
  }

  def processMessage(msg: T)

}
