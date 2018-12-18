package parameter.server.kafka.logic

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import scala.util.Random
import org.junit.Test
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import parameter.server._
import parameter.server.communication.Messages
import parameter.server.communication.Messages.{Message, Pull, PullAnswer, Push}
import parameter.server.kafka.logic.server.AsynchronousServerLogic
import parameter.server.utils.{Types, Vector, Utils}
import parameter.server.kafka.logic.AsynchronousServerLogicTest._

class AsynchronousServerLogicTest extends FlatSpec with PropertyChecks with Matchers {

  "Asynchronous stateless server logic" should " answer to pull requests" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    CollectSink.values.clear()
    
    val data = generateInput(numTest, seed => Pull[Int, Int, Vector](r.nextInt(numTest), r.nextInt(numTest)))

    val ds: DataStream[Message[Int, Int, Vector]] = env
      .fromCollection(data)

    val output = ds
      .process(new AsynchronousServerLogic[Int, Int, Vector] {
        override val model: ValueState[Vector] = null

        override def onPullReceive(pull: Pull[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = {
          val payload: Vector = udf(pull.dest, pull.src)
          out.collect(Right(PullAnswer(pull.destination, pull.source, payload)))
        }

        override def onPushReceive(push: Messages.Push[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = ???
      })
      
      val (_, msgStream) = Utils.splitStream(output)

      msgStream
      .addSink(new CollectSink[Message[Int, Int, Vector]])


    env.execute()

    CollectSink.values.toList.sortBy(m => (m.destination, m.source)) shouldBe data.map(x => PullAnswer(x.dest, x.src, udf(x.dest, x.src))).sortBy(m => (m.destination, m.source))
  }

  "Asnychronous server logic" should "update its state based on push messages" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    CollectSink.values.clear()
    
    val data = generateInput(numTest, seed => Push[Int, Int, Vector](r.nextInt(numTest), r.nextInt(numTest), Vector(Array.fill(vectorSize)(scala.util.Random.nextDouble))))

    val ds: DataStream[Message[Int, Int, Vector]] = env
      .fromCollection(data)

    val output = ds
      .keyBy(p => p.destination)
      .process(new AsynchronousServerLogic[Int, Int, Vector] {
        override lazy val model: ValueState[Vector] = getRuntimeContext.getState(
        new ValueStateDescriptor[Vector]("shared parameters", classOf[Vector]))

        override def onPullReceive(pull: Pull[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = ???

        override def onPushReceive(push: Messages.Push[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = {
          val p = getOrElseUpdate(Vector(Array.fill(vectorSize)(0.0)))

          val update = Vector.vectorSum(p, push.msg)

          model.update(update)

          out.collect(Right(PullAnswer(push.destination, push.source, update)))
        }
      })

    val (_, msgStream) = Utils.splitStream(output)

    msgStream
      .addSink(new CollectSink[Message[Int, Int, Vector]])

    env.execute

    val target = data
      .groupBy(p => p.destination)
      .map({
        case(k,v) => 
        k -> v.foldLeft(Vector(Array.fill(vectorSize)(0.0))){(acc, i) => Vector.vectorSum(acc, i.msg)}})

    val result = CollectSink.values
      .groupBy(m => m.source)
      .map({
        case(k,v) => 
        k -> (v flatMap(_.message) maxBy(Vector.vectorLengthSqr))
      })

    result shouldBe target
  }

  "Asynchronous server logic" should "write to output" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    CollectSink.values.clear()
    
    val data = generateInput(numTest, seed => Pull[Int, Int, Vector](r.nextInt(numTest), r.nextInt(numTest)))

    val ds: DataStream[Message[Int, Int, Vector]] = env
      .fromCollection(data)

    val output = ds
      .process(new AsynchronousServerLogic[Int, Int, Vector] {
        override val model: ValueState[Vector] = null

        override def onPullReceive(pull: Pull[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = {
          val payload: Vector = udf(pull.dest, pull.src)
          out.collect(Left(DummyOutput(payload)))
        }

        override def onPushReceive(push: Messages.Push[Int, Int, Vector], out: Collector[Either[Types.ParameterServerOutput, Message[Int, Int, Vector]]]): Unit = ???
      })
      
      val (outStream, _) = Utils.splitStream(output)

      outStream
      .addSink(new CollectOutputSink[Types.ParameterServerOutput])


    env.execute()

    CollectOutputSink.values.toList.map(_.asInstanceOf[DummyOutput].v).sortBy(Vector.vectorLengthSqr) shouldBe data.map(x => udf(x.dest, x.src)).sortBy(Vector.vectorLengthSqr)
    
  }
}

object AsynchronousServerLogicTest {
  
    val r = new scala.util.Random
    lazy val numTest = 20
    lazy val vectorSize = 5

    val udf: (Int, Int) => Vector = (x,y) => Vector(Array(x.toDouble, y.toDouble, math.pow(x,y))) 

    case class DummyOutput(v: Vector) extends Types.ParameterServerOutput

  /**
    * Generate n input with the provided function
    * @param n: Number of generated instances
    * @param f: Function to instanstiate input
    * @return List of inputs
    */
  def generateInput[A](n: Int, f: Int => A): List[A] = {
    for{
      seed <- (0 until n).toList
    } yield f(seed)
  }

}
