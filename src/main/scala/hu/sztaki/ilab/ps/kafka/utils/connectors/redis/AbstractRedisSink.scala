package hu.sztaki.ilab.ps.kafka.utils.connectors.redis

import scredis.{Redis, RedisCluster, Server}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

abstract class AbstractRedisSink[T](host: String, port: Integer) extends RichSinkFunction[T] {

  lazy val redisClient = new Redis(host, port)
  lazy val redisClusterClient = RedisCluster(Server(host, port))

  override def invoke(msg: T) = {
    processMessage(msg)
  }

  def processMessage(msg: T)

}
