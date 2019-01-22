package parameter.server

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import parameter.server.algorithms.Metrics
import parameter.server.algorithms.matrix.factorization.MfPsFactory
import parameter.server.algorithms.matrix.factorization.RecSysMessages.{EvaluationOutput, EvaluationRequest}
import parameter.server.communication.Messages._
import parameter.server.utils.Types.Recommendation
import parameter.server.utils.Utils
import parameter.server.utils.datastreamlogger.{DbWriterFactory, JobLogger}

class OnlineTrainAndEval extends Serializable {

  case class Result(evaluationId: Long, nDCG: Double, timestamp: Long)
  case class AccumulatedResult(nDCG: Double, timeSlot: Long, count: Int) {
    override def toString: String =
      s"$timeSlot,$nDCG,$count"
  }



 def createPs(algorithm:String, psImplType: String,
              parameters: ParameterTool,
              inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton[EvaluationRequest] = algorithm match {
   case "matrixFactorization" => MfPsFactory.createPs(psImplType, parameters, inputStream, env)
   case _ => throw new UnsupportedOperationException
 }



  /** Run PS with CLI argument
    *
    */
  def parameterParseAndRun(args: Array[String]): Unit =
    Utils.getParameters(args) match {
      case Some(parameters) =>
        val parallelism = parameters.getInt("parallelism")

        val inputFile = parameters.get("inputFile")
        val outputFile = parameters.get("outputFile")
        // period of final NDCG evaluation
        val snapshotLength = parameters.getInt("snapshotLength", 86400)

        val withMeasureFrame = parameters.getBoolean("withDataStreamLogger", false)
        val dbBackend = parameters.get("dbBackend", "couchbase")
        // impl / dbms / kafkaredis
        val psImplType = parameters.get("psImplType")
        val algorithm = parameters.get("algorithm", "matrixFactorization")
        val K = parameters.getInt("K")
        val doEvalAndWrite = parameters.getBoolean("doEvalAndWrite", true)

        run(algorithm, psImplType, parallelism, inputFile, outputFile, snapshotLength, doEvalAndWrite, withMeasureFrame, dbBackend, K, parameters)
      case None =>
    }

  /** Run PS
    *
    */
  def run(algorithm: String,
          psImplType: String,
          parallelism: Int,
          inputFile: String,
          outputFile: String,
          snapshotLength: Int,
          doEvalAndWrite: Boolean,
          withDataStreamLogger: Boolean,
          dbBackend: String,
          K: Int,
          parameters: ParameterTool): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    val psOutput =  if (withDataStreamLogger) {
      JobLogger.doWithLogging[EvaluationRequest, Recommendation](createInput(env, inputFile),
        DbWriterFactory.createDbWriter(dbBackend, parameters), env, psImplType,
        (input, env) => runPS(createPs(algorithm, psImplType, parameters, input, env), K ,parallelism),
          _.evaluationId, _.evaluationId
      )
    } else {
      runPS(createPs(algorithm, psImplType, parameters, createInput(env, inputFile), env), K, parallelism)
    }

    if(doEvalAndWrite) {
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


  private def runPS(ps: ParameterServerSkeleton[EvaluationRequest], K: Int, parallelism: Int): DataStream[Recommendation] =
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

}


object OnlineTrainAndEval {
  def main(args: Array[String]): Unit = {
    //val params = Utils.getParameters(args)
    val model = new OnlineTrainAndEval()
    model.parameterParseAndRun(args)
  }
}
