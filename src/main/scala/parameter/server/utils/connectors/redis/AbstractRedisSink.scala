package parameter.server.utils.connectors.redis

import scredis.{RedisCluster, Server}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

abstract class AbstractRedisSink[T](host: String, port: Integer) extends RichSinkFunction[T] {

  lazy val redisClient = RedisCluster(Server(host, port))

  override def invoke(msg: T) = {
    processMessage(msg)
  }

  def processMessage(msg: T)

}
