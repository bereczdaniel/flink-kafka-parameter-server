package parameter.server.algorithms.matrix.factorization

import eu.streamline.hackathon.flink.scala.job.parameter.server.factors.RangedRandomFactorInitializerDescriptor
import org.apache.flink.streaming.api.scala._
import parameter.server.ParameterServer
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.communication.Messages._
import parameter.server.utils.IDGenerator

object OnlineTrainAndEval {

  def main(args: Array[String]): Unit = {
    val K = 100
    val parallelism = 4
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)


    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(10, -0.001, 0.001)

    val source = env
      .readTextFile("data/lastFM/perf.csv")
      .map(line => {
        val fields = line.split(",")
        EvaluationRequest(fields(1).toInt, fields(2).toInt, IDGenerator.next, 1.0, fields(0).toLong)
      })

    val ps = new ParameterServer(
      env,
      src = source,
      workerLogic = new TrainAndEvalWorkerLogic, serverLogic = new TrainAndEvalServerLogic,
      serverToWorkerParse = pullAnswerFromString, workerToServerParse = workerToServerParse,
      host = "localhost:", port = 9093, serverToWorkerTopic = "serverToWorkerTopic", workerToServerTopic = "workerToServerTopic",
      broadcastServerToWorkers = true)

    ps
      .start()
      .print()

    env.execute()

  }

  def workerToServerParse(line: String): Message = {
    val fields = line.split(":")

    fields.head match {
      case "Pull" => Pull(fields(1).toInt, fields(2).toInt)
      case "Push" => Push(fields(1).toInt, fields(2).toInt, Some(Vector(fields(3).split(",").map(_.toDouble))))
      case _ =>
        throw new NotSupportedMessage
        null
    }
  }

  def pullAnswerFromString(line: String): PullAnswer = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toInt, Vector(fields(2).split(",").map(_.toDouble)))
  }
}
