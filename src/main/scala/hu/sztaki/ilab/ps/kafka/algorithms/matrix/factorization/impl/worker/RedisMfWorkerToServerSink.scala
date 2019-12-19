package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.worker

import parameter.server.communication.Messages.{Message, NotSupportedMessage, Pull, Push}
import parameter.server.utils.Utils
import parameter.server.utils.connectors.redis.AbstractRedisSink
import matrix.factorization.types.Vector
import scredis.protocol.Decoder
import scredis.serialization._

import scala.util.control.Exception.Catcher

class RedisMfWorkerToServerSink(host: String, port: Int, channelName: String, numFactors: Int, rangeMin: Double, rangeMax: Double)
    extends AbstractRedisSink[Message[Long, Int, Vector]](host, port) {

  import redisClient.dispatcher

  lazy val pullScriptId = redisClient.scriptLoad(Utils.loadTextFileContent("/scripts/redis/pull_user_vector.lua"))
  lazy val pushScriptId = redisClient.scriptLoad(Utils.loadTextFileContent("/scripts/redis/push_update_user_vector.lua"))

  private implicit lazy val UnitDecoder: Decoder[Unit] = { case _ => () }
  private implicit lazy val writer: Writer[Any] = AnyWriter
  val catcher: Catcher[Unit] = {
    case e: Exception =>
      e.printStackTrace()
      throw e
  }

  override def processMessage(msg: Message[Long, Int, Vector]) = {

    msg match {
      case Pull(evaluationId, userId) =>
        // Query user vector from db & send to channel for broadcasting:
        pullScriptId.map { scriptId =>
          redisClusterClient.evalSHA[Unit, Int, Any](scriptId, List(msg.destination),
            List(msg.source, channelName, numFactors, rangeMin, rangeMax))
          //println("pull requested from db")
        }.onFailure(catcher)

      case Push(evaluationId, userId, vect) =>
        // update user vector in db with userDelta
        pushScriptId.map { scriptId =>
          redisClusterClient.evalSHA[Unit, Int, Any](scriptId, List(msg.destination), msg.message.get.value.toList)
          //println("push submitted to db")
        }.onFailure(catcher)

      case _ => throw new NotSupportedMessage
    }
  }

}
