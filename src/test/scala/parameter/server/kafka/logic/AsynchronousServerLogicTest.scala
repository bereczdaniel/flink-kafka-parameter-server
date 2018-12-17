package parameter.server.kafka.logic

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import scala.util.Random
import org.junit.Test
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import parameter.server.CollectSink
import parameter.server.communication.Messages
import parameter.server.communication.Messages.{Message, Pull, PullAnswer}
import parameter.server.kafka.logic.server.AsynchronousServerLogic
import parameter.server.utils.{Types, Vector}

class AsynchronousServerLogicTest extends FlatSpec with PropertyChecks with Matchers {

  
    val r = new scala.util.Random
    val numTest = 10

  def generateInput[A](n: Int, f: Int => A): List[A] = {
    for{
      seed <- (0 until n).toList
    } yield f(seed)
  }

  "Asynchronous server logic" should " answer to pull requests" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    CollectSink.values.clear()
    
    val data = generateInput(numTest, seed => Pull[Int, Int, Vector](seed, r.nextInt(numTest)))

    val ds: DataStream[Message[Int, Int, Vector]] = env
      .fromCollection(data)

    lazy val udf: (Int, Int) => Vector = (x,y) => Vector(Array(x.toDouble, y.toDouble, math.pow(x,y))) 

    ds
      .process(new AsynchronousServerLogic[Int, Int, Vector] {
        override val model: ValueState[Vector] = null

        lazy val udf: (Int, Int) => Vector = (x,y) => Vector(Array(x.toDouble, y.toDouble, math.pow(x,y))) 

        override def onPullReceive(pull: Pull[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = {
          val payload: Vector = udf(pull.dest, pull.src)
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


    assert(
      CollectSink.values.toList.sortBy(_.destination) == 
      data.map(x => PullAnswer(x.dest, x.src, udf(x.dest, x.src))).sortBy(_.destination))
  }

}
