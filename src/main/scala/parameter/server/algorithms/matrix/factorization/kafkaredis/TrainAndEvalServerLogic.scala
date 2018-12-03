package parameter.server.algorithms.matrix.factorization.kafkaredis

import com.redis._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector
import parameter.server.communication.Messages
import parameter.server.communication.Messages.PullAnswer
import parameter.server.kafkaredis.logic.server.SynchronousServerLogic
import parameter.server.utils.Types.ItemId
import parameter.server.utils.{Types, Vector}

class TrainAndEvalServerLogic(_init: Int => Vector, _update: (Vector, Vector) => Vector)  extends SynchronousServerLogic[Long, Int, Vector] {
  // WARNING !! NOT USED: - only kept because class must implement as abstract field/method - can be deleted...
  override lazy val model: ValueState[Vector] = getRuntimeContext.getState(
    new ValueStateDescriptor[Vector]("shared parameters", classOf[Vector]))

  @transient lazy val r = new RedisClient("localhost", 6379)

  @transient lazy val init: Int => Vector = _init
  @transient lazy val update: (Vector, Vector) => Vector = _update

  override def onPullReceive(pull: Messages.Pull[Long, Int, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Int, Long, Vector]]]): Unit = {
    //// original code with state:
    //out.collect(Right(PullAnswer(pull.dest, pull.src, getOrElseUpdate(init(pull.dest)))))

    //DEBUG:
    //println("Querying user vector " + pull.dest)

    val valueMapOpt =  r.hgetall1(pull.dest)

    val valueVect: Vector = valueMapOpt match {
      case Some(valueMap) =>
        var valueVectTmp = Vector(valueMap.keys.size)
        for (i <- valueMap.keys) {
          valueVectTmp.value(i.toInt) = valueMap.get(i).get.toDouble
        }
        //DEBUG:
        //println("Queried user vector " + pull.dest + ": " + valueVectTmp.toString, " = ", valueMap.toString())

        valueVectTmp
      case None =>
        val valueVectTmp = init(pull.dest)

        //DEBUG:
        //print("Initialized user vector " + pull.dest + ": ")
        //valueVectTmp.value.zipWithIndex.map(e => (e._2,e._1)).foreach(e => print(e._1," -> ", e._2))

        r.hmset(pull.dest, valueVectTmp.value.zipWithIndex.map(e => (e._2,e._1)))
        valueVectTmp
    }
    out.collect(Right(PullAnswer(pull.dest, pull.src, valueVect)))
  }

  override def onPushReceive(push: Messages.Push[Long, Int, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Int, Long, Vector]]]): Unit = {
    //// original code with state:
    //val oldParam = model.value()
    //model.update(update(oldParam, push.msg))

    // WARNING: !! WE DON'T USE THE update FUNCTION SINCE THE INCREMENT IS DONE BY THE DB
    for (i <- push.msg.value.indices) {
      r.hincrbyfloat(push.dest, i, push.msg.value(i).toFloat)
    }
  }
}

object TrainAndEvalServerLogic {
  def apply(_init: ItemId => Vector, _update: (Vector, Vector) => Vector): TrainAndEvalServerLogic = new TrainAndEvalServerLogic(_init, _update)
}