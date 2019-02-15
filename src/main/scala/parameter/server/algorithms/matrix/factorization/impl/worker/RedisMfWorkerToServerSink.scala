package parameter.server.algorithms.matrix.factorization.impl.worker

import parameter.server.communication.Messages.{Message, NotSupportedMessage, Pull, Push}
import parameter.server.utils.{Utils, Vector}
import parameter.server.utils.connectors.redis.AbstractRedisSink
import scredis.protocol.Decoder
import scredis.serialization._

class RedisMfWorkerToServerSink(host: String, port: Int, channelName: String, numFactors: Int, rangeMin: Double, rangeMax: Double)
    extends AbstractRedisSink[Message[Long, Int, Vector]](host, port) {

  lazy val pullScriptId = redisClient.scriptLoad(Utils.loadTextFileContent("/scripts/redis/pull_user_vector.lua"))
  lazy val pushScriptId = redisClient.scriptLoad(Utils.loadTextFileContent("/scripts/redis/push_update_user_vector.lua"))

  private implicit lazy val UnitDecoder: Decoder[Unit] = { case _ => () }
  private implicit lazy val writer: Writer[Any] = AnyWriter

  private var asyncOpCounter = 0

  override def processMessage(msg: Message[Long, Int, Vector]) = {
    import redisClient.dispatcher

    msg match {
      case Pull(evaluationId, userId) =>
        // Query user vector from db & send to channel for broadcasting:
        pullScriptId.map { scriptId =>
          asyncOpCounter+=1
          redisClient.evalSHA[Unit, Int, Any](scriptId, List(msg.destination),
            List(msg.source, channelName, numFactors, rangeMin, rangeMax))
        }

      case Push(evaluationId, userId, vect) =>
        // update user vector in db with userDelta
        pushScriptId.map { scriptId =>
          asyncOpCounter+=1
          redisClient.evalSHA[Unit, Int, Any](scriptId, List(msg.destination), msg.message.get.value.toList)
        }

      case _ => throw new NotSupportedMessage
    }
  }

  override def close() = {
    while (asyncOpCounter < 1) {
      println(asyncOpCounter)
    }
  }

}
