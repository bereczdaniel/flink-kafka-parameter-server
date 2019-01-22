package parameter.server.algorithms.matrix.factorization.impl.worker

import parameter.server.communication.Messages.{Message, NotSupportedMessage, Pull, Push}
import parameter.server.utils.{Utils, Vector}
import parameter.server.utils.connectors.redis.AbstractRedisSink

class RedisMfWorkerToServerSink(host: String, port: Int, channelName: String, numFactors: Int, rangeMin: Double, rangeMax: Double)
    extends AbstractRedisSink[Message[Long, Int, Vector]](host, port) {

  lazy val pullScriptId = redisClient.scriptLoad(Utils.loadScriptContent("/scripts/redis/pull_user_vector.lua"))
  lazy val pushScriptId = redisClient.scriptLoad(Utils.loadScriptContent("/scripts/redis/push_update_user_vector.lua"))

  override def processMessage(msg: Message[Long, Int, Vector]) = {
    msg match {
      case Pull(evaluationId, userId) =>
        // Query user vector from db & send to channel for broadcasting:
        redisClient.evalSHA(pullScriptId.get, List(msg.destination),
          List(msg.source, channelName, numFactors, rangeMin, rangeMax))

      case Push(evaluationId, userId, vect) =>
        // update user vector in db with userDelta
        redisClient.evalSHA(pushScriptId.get, List(msg.destination), msg.message.get.value.toList)

      case _ => throw new NotSupportedMessage
    }
  }

}
