package parameter.server.algorithms.matrix.factorization

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
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
import parameter.server.algorithms.matrix.factorization.kafka.OnlineTrainAndEval.{AccumulatedResult, Recommendation, Result}
import parameter.server.communication.Messages._
import parameter.server.utils.Types.{ParameterServerOutput, ParameterServerSkeleton}
import parameter.server.utils.{Utils, Vector}
import parameter.server.utils.datastreamlogger.LogFrame
import parameter.server.utils.datastreamlogger.impl.{ConsoleWriter, CouchBaseWriter}

abstract class AbstractOnlineTrainAndEval extends Serializable {

  // abstract function
  // the type of the test process run (e.g. kafka/redis/...)
  def testProcessCategory: String
  // the recipe of the given implementation of the PS
  def createPS(parameters: ParameterTool)(inputStream: DataStream[EvaluationRequest]): ParameterServerSkeleton

  /** Get the logging backenbd db
    *
    * @param dbBackend
    * @param parameters
    * @return
    */
  def getDBWriter(dbBackend: Option[String] = None, parameters: ParameterTool) = dbBackend match {
    case Some(db) if db != "couchbase"  =>  db match  {
      case "console" => new ConsoleWriter()
      case "postgresql" => throw new UnsupportedOperationException
      case _ => throw new UnsupportedOperationException
    }
    case _ => CouchBaseWriter.getFromParameters(parameters)
  }

  /** Run PS with CLI argument
    *
    * @param args
    */
  def parameterParseAndRun(args: Array[String]): Unit = {
    val params = Utils.getParameters(args)

    for (
      pt <- params
    ) {
      val learningRate = pt.getDouble("learningRate")
      val negativeSampleRate = pt.getInt("negativeSampleRate")
      val numFactors = pt.getInt("numFactors")
      val rangeMin = pt.getDouble("rangeMin")
      val rangeMax = pt.getDouble("rangeMax")
      val K = pt.getInt("K")
      val workerK = pt.getInt("workerK")
      val bucketSize = pt.getInt("bucketSize")
      val parallelism = pt.getInt("parallelism")

      val inputFile = pt.get("inputFile")
      val outputFile = pt.get("outputFile")
      val snapshotLength = pt.getInt("snapshotLength")

      val measureFrame = Option(pt.getBoolean("measureFrame"))
      val dbBackend = Option(pt.get("dbBackend"))

      run(K, snapshotLength, learningRate, numFactors, negativeSampleRate, rangeMin, rangeMax, inputFile,
        workerK, bucketSize, outputFile, measureFrame, dbBackend, parallelism, params)
      )
    }
  }

  /** Run PS
    *
    * @param K
    * @param snapshotLength
    * @param learningRate
    * @param numFactors
    * @param negativeSampleRate
    * @param rangeMin
    * @param rangeMax
    * @param inputFile
    * @param workerK
    * @param bucketSize
    * @param outputFile
    * @param measureFrame
    * @param dbBackend
    * @param parallelism
    * @param parameters
    */
  def run(K: Int, snapshotLength: Int,
          learningRate: Double, numFactors: Int, negativeSampleRate: Int, rangeMin: Double, rangeMax: Double,
          inputFile: String,
          workerK: Int, bucketSize: Int,
          outputFile: String,
          measureFrame: Option[Boolean] = Some(true),
          dbBackend: Option[String],
          parallelism: Int, parameters: ParameterTool): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)

    val psOutput =  if (measureFrame.isEmpty || measureFrame.get) {
      LogFrame.addLogFrameAndRunPS(createInput(env, inputFile), createPS(parameters), getDBWriter(dbBackend, parameters), env, testProcessCategory)
    } else {
      runPS(createPS(parameters)(createInput(env, inputFile)))
    }

    val mergedRecommendations = merge(psOutput, K, parallelism)

    val results = eval(mergedRecommendations)

    val accumulatedResults = accumulateResults(results, snapshotLength)

    saveToFile(accumulatedResults, outputFile)

    env.execute()
  }

  private def createInput(env: StreamExecutionEnvironment, fileName: String): DataStream[EvaluationRequest] =
    env
      .readTextFile("lastFM/sliced/first_10_idx")
      .map(line => {
        val fields = line.split(",")
        EvaluationRequest(fields(2).toInt, fields(3).toInt, fields(0).toLong, 1.0, fields(1).toLong - 1390209861L)
      })


  private def runPS(ps: ParameterServerSkeleton): DataStream[EvaluationOutput] =
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

  private def merge(psOut: DataStream[EvaluationOutput], K: Int, parallelism: Int): DataStream[Recommendation] =
    psOut
    .keyBy(_.evaluationId)
    .countWindow(parallelism)
    .process(new ProcessWindowFunction[EvaluationOutput, Recommendation, Long, GlobalWindow] {
      override def process(key: Long, context: Context,
                           elements: Iterable[EvaluationOutput],
                           out: Collector[Recommendation]): Unit = {

        val target = elements.map(_.itemId).max
        val topK = elements.flatMap(_.topK).toList.sortBy(_._2).distinct.takeRight(K).map(_._1)
        val id = elements.head.evaluationId
        val ts = elements.map(_.ts).max
        out.collect(Recommendation(target, topK, id, ts))
      }
    })

  private def eval(recommendations: DataStream[Recommendation]): DataStream[Result] =
    recommendations
    .map(rec => {
      val nDCG = Metrics.nDCG(rec.topK, rec.targetId)
      Result(rec.evaluationId, nDCG, rec.timestamp)
    })

  private def accumulateResults(results: DataStream[Result], snapshotLength: Long): DataStream[AccumulatedResult] =
    results
      .keyBy(r => r.timestamp / snapshotLength)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(60)))
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

  /** Util function to kafka communication
    *
    * @param line
    * @return
    */
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

  /** Util function to create PS
    *
    * @param line
    * @return
    */
  private def pullAnswerFromString(line: String): PullAnswer[Long, Int, Vector] = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toLong, Vector(fields(2).split(",").map(_.toDouble)))
  }
}


