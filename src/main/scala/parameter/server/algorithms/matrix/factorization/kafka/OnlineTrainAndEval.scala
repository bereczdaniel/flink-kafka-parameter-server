package parameter.server.algorithms.matrix.factorization.kafka

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import parameter.server.algorithms.Metrics
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.RecSysMessages.{EvaluationOutput, EvaluationRequest}
import parameter.server.algorithms.matrix.factorization.kafka.OnlineTrainAndEval.{AccumulatedResult, Result}
import parameter.server.communication.Messages._
import parameter.server.kafka.ParameterServer
import parameter.server.utils.Types.ParameterServerOutput
import parameter.server.utils.{Utils, Vector}

class OnlineTrainAndEval() extends Serializable {

  def run(K: Int, snapshotLength: Int,
          learningRate: Double, n: Int, negativeSampleRate: Int, rangeMin: Double, rangeMax: Double,
          inputFile: String,
          workerK: Int, bucketSize: Int,
          hostName: String, port: Int, serverToWorkerTopic: String, workerToServerTopic: String,
          broadcastServerToWorkers: Boolean,
          outputFile: String,
          parallelism: Int): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(n, rangeMin, rangeMax)

    val ps = new ParameterServer[EvaluationRequest, Vector, Long, Int](
      env,
      inputStream = createInput(env, inputFile),
      workerLogic = new TrainAndEvalWorkerLogic(n, learningRate, negativeSampleRate, rangeMin, rangeMax, workerK,bucketSize),
      serverLogic = new TrainAndEvalServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum),
      serverToWorkerParse = pullAnswerFromString, workerToServerParse = workerToServerParse,
      host = hostName, port = port, serverToWorkerTopic, workerToServerTopic,
      broadcastServerToWorkers = broadcastServerToWorkers)

    val psOutput = runPS(ps)

    val mergedRecommendations = mergeAndEval(psOutput, K, parallelism)

    val accumulatedResults = accumulateResults(mergedRecommendations, snapshotLength)

    saveToFile(accumulatedResults, outputFile)

    env.execute()
  }

  private def createInput(env: StreamExecutionEnvironment, fileName: String): DataStream[EvaluationRequest] =
    env
    .readTextFile(fileName)
    .map(line => {
      val fields = line.split(",")
      EvaluationRequest(fields(2).toInt, fields(3).toInt, fields(0).toLong, 1.0, fields(1).toLong)
    })

  private def runPS(ps: ParameterServer[EvaluationRequest, Vector, Long, Int]): DataStream[EvaluationOutput] =
    ps
    .start()
    .flatMap(new FlatMapFunction[ParameterServerOutput, EvaluationOutput] {
      override def flatMap(value: ParameterServerOutput, out: Collector[EvaluationOutput]): Unit = {
        value match {
          case eval: EvaluationOutput => out.collect(eval)
          case _ => throw new NotSupportedOutput
        }
      }
    })

  private def mergeAndEval(psOut: DataStream[EvaluationOutput], K: Int, parallelism: Int): DataStream[Result] =
    psOut
      .keyBy(_.evaluationId)
      .countWindow(parallelism)
      .process(new ProcessWindowFunction[EvaluationOutput, Result, Long, GlobalWindow] {
        override def process(key: Long, context: Context,
                             elements: Iterable[EvaluationOutput],
                             out: Collector[Result]): Unit = {
          val finalTopK = elements.flatMap(_.topK).toList.sortBy(_._2).takeRight(K).map(_._1)
          val nDCG = Metrics.ndcg(finalTopK, elements.head.itemId)

          out.collect(Result(key, nDCG, elements.head.ts))
        }
      })

  private def accumulateResults(results: DataStream[Result], snapshotLength: Long): DataStream[AccumulatedResult] =
    results
      .keyBy(r => r.timestamp / snapshotLength)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .process(new ProcessWindowFunction[Result, AccumulatedResult, Long, TimeWindow] {
        override def process(key: Long, context: Context,
                             elements: Iterable[Result],
                             out: Collector[AccumulatedResult]): Unit = {
          val count = elements.size
          val avg_nDCG: Double = elements.map(_.nDCG).sum / count

          out.collect(AccumulatedResult(avg_nDCG, key, count))
        }
      })

  private def saveToFile(accumulatedResults: DataStream[AccumulatedResult], outputFile: String): Unit = {
    accumulatedResults
      .map(_.timeSlot)
      .print()

    accumulatedResults
      .writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)
  }

  private def workerToServerParse(line: String): Message[Long, Int, Vector] = {
    val fields = line.split(":")

    fields.head match {
      case "Pull" => Pull(fields(1).toLong, fields(2).toInt)
      case "Push" => Push(fields(1).toLong, fields(2).toInt, Vector(fields(3).split(",").map(_.toDouble)))
      case _ =>
        throw new NotSupportedMessage
        null
    }
  }

  private def pullAnswerFromString(line: String): PullAnswer[Long, Int, Vector] = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toLong, Vector(fields(2).split(",").map(_.toDouble)))
  }
}

object OnlineTrainAndEval {

  case class Result(evaluationId: Long, nDCG: Double, timestamp: Long)
  case class AccumulatedResult(nDCG: Double, timeSlot: Long, count: Int) {
    override def toString: String =
      s"$timeSlot,$nDCG,$count"
  }

  def main(args: Array[String]): Unit = {
    val params = Utils.getParameters(args)

    for(
      pt <- params
    ){
      val learningRate = pt.get("learningRate").toDouble
      val negativeSampleRate = pt.get("negativeSampleRate").toInt
      val numFactors = pt.get("numFactors").toInt
      val rangeMin = pt.get("rangeMin").toDouble
      val rangeMax = pt.get("rangeMax").toDouble
      val K = pt.get("K").toInt
      val workerK = pt.get("workerK").toInt
      val bucketSize = pt.get("bucketSize").toInt
      val parallelism = pt.get("parallelism").toInt

      val inputFile = pt.get("inputFile")
      val outputFile = pt.get("outputFile")
      val snapshotLength = pt.get("snapshotLength").toInt

      val hostName = pt.get("host")
      val port = pt.get("port").toInt
      val serverToWorkerTopic = pt.get("serverToWorkerTopic")
      val workerToServerTopic = pt.get("workerToServerTopic")

      val broadcast = pt.get("broadcast").contains("y")

      val model = new OnlineTrainAndEval()

      model.run(K, snapshotLength, learningRate, numFactors, negativeSampleRate, rangeMin, rangeMax, inputFile,
        workerK, bucketSize, hostName, port, serverToWorkerTopic, workerToServerTopic, broadcast, outputFile, parallelism
      )
    }
  }
}
