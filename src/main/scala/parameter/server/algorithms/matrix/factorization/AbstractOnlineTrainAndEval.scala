package parameter.server.algorithms.matrix.factorization

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
import parameter.server.communication.Messages._
import parameter.server.utils.Types.{ParameterServerSkeleton, Recommendation}
import parameter.server.utils.{Utils, Vector}
import parameter.server.utils.datastreamlogger.{DbWriter, LogFrame}
import parameter.server.utils.datastreamlogger.impl.{ConsoleWriter, CouchBaseWriter}

abstract class AbstractOnlineTrainAndEval extends Serializable {

  case class Result(evaluationId: Long, nDCG: Double, timestamp: Long)
  case class AccumulatedResult(nDCG: Double, timeSlot: Long, count: Int) {
    override def toString: String =
      s"$timeSlot,$nDCG,$count"
  }


  // abstract function
  // the type of the test process run (e.g. kafka/redis/...)
  def testProcessCategory: String
  // the recipe of the given implementation of the PS
  def createPS(learningRate: Double, numFactors: Int, negativeSampleRate: Int, rangeMin: Double, rangeMax: Double,
               workerK: Int, bucketSize: Int,
               parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
               inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton

  /** Get the logging backenbd db
    *
    * @return
    */
  def getDBWriter(dbBackend: Option[String] = None, parameters: ParameterTool): DbWriter = dbBackend match {
    case Some(db) if db != "couchbase"  =>  db match  {
      case "console" => new ConsoleWriter()
      case "postgresql" =>
        // TODO
        throw new UnsupportedOperationException
      case _ => throw new UnsupportedOperationException
    }
    case _ => CouchBaseWriter.getFromParameters(parameters)
  }

  /** Run PS with CLI argument
    *
    */
  def parameterParseAndRun(args: Array[String]): Unit =
    Utils.getParameters(args) match {
      case Some(parameters) =>
        val learningRate = parameters.getDouble("learningRate")
        val negativeSampleRate = parameters.getInt("negativeSampleRate")
        val numFactors = parameters.getInt("numFactors")
        val rangeMin = parameters.getDouble("rangeMin")
        val rangeMax = parameters.getDouble("rangeMax")
        val K = parameters.getInt("K")
        val workerK = parameters.getInt("workerK")
        val bucketSize = parameters.getInt("bucketSize")
        val parallelism = parameters.getInt("parallelism")

        val inputFile = parameters.get("inputFile")
        val outputFile = parameters.get("outputFile")
        val snapshotLength = parameters.getInt("snapshotLength", 86400)

        val measureFrame = Option(parameters.getBoolean("measureFrame"))
        val dbBackend = Option(parameters.get("dbBackend"))
        val evalAndWrite = parameters.getBoolean("evalAndWrite", true)

        run(K, snapshotLength, learningRate, numFactors, negativeSampleRate, rangeMin, rangeMax, inputFile,
          workerK, bucketSize, outputFile, measureFrame, dbBackend, evalAndWrite, parallelism, parameters)
      case None =>
    }

  /** Run PS
    *
    */
  def run(K: Int, snapshotLength: Int,
          learningRate: Double, numFactors: Int, negativeSampleRate: Int, rangeMin: Double, rangeMax: Double,
          inputFile: String,
          workerK: Int, bucketSize: Int,
          outputFile: String,
          measureFrame: Option[Boolean] = Some(true),
          dbBackend: Option[String],
          evalAndWrite: Boolean,
          parallelism: Int,
          parameters: ParameterTool): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)

    val psOutput =  if (measureFrame.isEmpty || measureFrame.get) {
      LogFrame.addLogFrameAndRunPS(createInput(env, inputFile),
        getDBWriter(dbBackend, parameters), env, testProcessCategory,
        (input, env) => runPS(createPS(learningRate, numFactors, negativeSampleRate, rangeMin, rangeMax,
          workerK, bucketSize, parameters, factorInitDesc, input, env), K ,parallelism)

      )
    } else {
      runPS(createPS(learningRate, numFactors, negativeSampleRate, rangeMin, rangeMax,
        workerK, bucketSize, parameters, factorInitDesc, createInput(env, inputFile), env), K, parallelism)
    }

    if(evalAndWrite) {
      val results = eval(psOutput)
      val accumulatedResults = accumulateResults(results, snapshotLength)
      saveToFile(accumulatedResults, outputFile)
    }

    env.execute()
  }

  private def createInput(env: StreamExecutionEnvironment, fileName: String): DataStream[EvaluationRequest] =
    env
      .readTextFile("lastFM/sliced/first_10_idx")
      .map(line => {
        val fields = line.split(",")
        EvaluationRequest(fields(2).toInt, fields(3).toInt, fields(0).toLong, 1.0, fields(1).toLong - 1390209861L)
      })


  private def runPS(ps: ParameterServerSkeleton, K: Int, parallelism: Int): DataStream[Recommendation] =
    merge(ps
    .start()
    .flatMap(_ match {
      case eval: EvaluationOutput => Some(eval)
      case _ => throw new NotSupportedOutput
    }), K: Int, parallelism: Int)

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
    * @return
    */
  protected def workerToServerParse(line: String): Message[Long, Int, Vector] = {
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
    * @return
    */
  protected def pullAnswerFromString(line: String): PullAnswer[Long, Int, Vector] = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toLong, Vector(fields(2).split(",").map(_.toDouble)))
  }
}


