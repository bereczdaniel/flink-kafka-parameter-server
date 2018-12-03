package parameter.server.kafkaredis.logic

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import org.junit.Test
import org.scalatest.Matchers
import parameter.server.CollectSink
import parameter.server.communication.Messages
import parameter.server.communication.Messages.{Message, Pull, PullAnswer}
import parameter.server.kafkaredis.logic.server.AsynchronousServerLogic
import parameter.server.utils.{Types, Vector}

class AsynchronousServerLogicTest extends AbstractTestBase with Matchers {

  @Test
  def pullTest(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    CollectSink.values.clear()

    val data = List(
      Pull[Int, Int, Vector](1, 1),
      Pull[Int, Int, Vector](2, 1),
      Pull[Int, Int, Vector](3, 2),
      Pull[Int, Int, Vector](4, 2))

    val ds: DataStream[Message[Int, Int, Vector]] = env
      .fromCollection(data)


    ds
      .process(new AsynchronousServerLogic[Int, Int, Vector] {
        override val model: ValueState[Vector] = null

        override def onPullReceive(pull: Pull[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = {
          val payload: Vector = Vector(Array(pull.dest.toDouble, pull.src.toDouble, math.pow(pull.dest, pull.src)))
          out.collect(Right(PullAnswer(pull.destination, pull.source, payload)))
        }

        override def onPushReceive(push: Messages.Push[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = ???
      })
      .flatMap(new FlatMapFunction[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]], Message[Int, Int, Vector]] {
        override def flatMap(value: Either[Types.ParameterServerOutput, Message[Int, Int, Vector]], out: Collector[Message[Int, Int, Vector]]): Unit = {
          value match {
            case Left(_) =>
            case Right(msg) => out.collect(msg)
          }
        }
      })
      .addSink(new CollectSink[Message[Int, Int, Vector]])


    env.execute()


    assert(CollectSink.values.toList.sortBy(_.destination) == data.map(x => PullAnswer(x.dest, x.src, Vector(Array(x.dest.toDouble, x.src.toDouble, math.pow(x.dest, x.src))))).sortBy(_.destination))
  }

}
