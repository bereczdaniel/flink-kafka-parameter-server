package parameter.server.algorithms.matrix.factorization.impl.server

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector
import types.{ItemId, Vector}
import parameter.server.communication.Messages
import parameter.server.communication.Messages.PullAnswer
import parameter.server.logic.server.AsynchronousServerLogic
import parameter.server.utils.Types
import scredis.{RedisCluster, Server}

class RedisBackedMfServerLogic(_init: Int => Vector, _update: (Vector, Vector) => Vector,
                               redisHost: String, redisPort: Int)  extends AsynchronousServerLogic[Long, Int, Vector] {
  // WARNING !! NOT USED: - only kept because class must implement as abstract field/method - can be deleted...
  override lazy val model: ValueState[Vector] = getRuntimeContext.getState(
    new ValueStateDescriptor[Vector]("shared parameters", classOf[Vector]))

  @transient lazy val r = RedisCluster(Server(redisHost, redisPort))

  @transient lazy val init: Int => Vector = _init
  @transient lazy val update: (Vector, Vector) => Vector = _update

  override def onPullReceive(pull: Messages.Pull[Long, Int, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Int, Long, Vector]]]): Unit = {
    //// original code with state:
    //out.collect(Right(PullAnswer(pull.dest, pull.inputStream, getOrElseUpdate(init(pull.dest)))))

    //DEBUG:
    //println("Querying user vector " + pull.dest)
    import r.dispatcher

    val valueMapFuture = r.hGetAll(pull.dest.toString)

    valueMapFuture.map { valueMapOpt =>
      val valueVect: Vector = valueMapOpt match {
        case Some(valueMap) =>
          var valueVectTmp = Vector(valueMap.keys.size)
          for (i <- valueMap.keys) {
            valueVectTmp.value(i.toInt) = valueMap(i).toDouble
          }
          //DEBUG:
          //println("Queried user vector " + pull.dest + ": " + valueVectTmp.toString, " = ", valueMap.toString())

          valueVectTmp
        case None =>
          val valueVectTmp = init(pull.dest)

          //DEBUG:
          //print("Initialized user vector " + pull.dest + ": ")
          //valueVectTmp.value.zipWithIndex.map(e => (e._2,e._1)).foreach(e => print(e._1," -> ", e._2))

          r.hmSet(pull.dest.toString, valueVectTmp.value.zipWithIndex.map(e => (e._2.toString, e._1.toString)).toMap)
          valueVectTmp
      }
      out.collect(Right(PullAnswer(pull.dest, pull.src, valueVect)))
    }
  }

  override def onPushReceive(push: Messages.Push[Long, Int, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Int, Long, Vector]]]): Unit = {
    //// original code with state:
    //val oldParam = model.value()
    //model.update(update(oldParam, push.msg))

    // WARNING: !! WE DON'T USE THE update FUNCTION SINCE THE INCREMENT IS DONE BY THE DB
    for (i <- push.msg.value.indices) {
      r.hIncrByFloat(push.dest.toString, i.toString, push.msg.value(i).toFloat)
    }
  }
}

object RedisBackedMfServerLogic {
  def apply(_init: ItemId => Vector, _update: (Vector, Vector) => Vector,
            redisHost: String, redisPort: Int): RedisBackedMfServerLogic =
    new RedisBackedMfServerLogic(_init, _update, redisHost, redisPort)
}