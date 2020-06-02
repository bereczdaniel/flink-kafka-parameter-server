package hu.sztaki.ilab.ps.common

import java.util.Properties

import data.stream.logger.{DbWriterFactory, JobLogger}
import hu.sztaki.ilab.ps.common.types.RecSysMessages.EvaluationRequest
import hu.sztaki.ilab.ps.common.types.{AccumulatedNdcgResult, GeneralIoProperties, GeneralMfProperties, ParameterServerOutput}
import matrix.factorization.types.Recommendation
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

abstract class PsWrapper {


  def buildPs(input: DataStream[EvaluationRequest], mfProperties: GeneralMfProperties, ioProperties: GeneralIoProperties,
              parameterTool: ParameterTool,
              env: StreamExecutionEnvironment):
  DataStream[_ <: ParameterServerOutput]


  def readInput(env: StreamExecutionEnvironment, ioProperties: GeneralIoProperties): DataStream[EvaluationRequest] =
    (ioProperties.inputMedia match {
      case Some(media) => media match {
        case "file" => env
          .readTextFile(ioProperties.inputFile.get) // "lastFM/sliced/first_10_idx"
        case "kafka" =>
          val properties = new Properties()
          properties.setProperty("bootstrap.servers", ioProperties.inputKafkaHostPort.get)
          properties.setProperty("group.id", "parameterServerInput")
          val kafkaSource = new FlinkKafkaConsumer011[String](ioProperties.inputKafkaTopic.get, new SimpleStringSchema(), properties).setStartFromLatest()
          env.addSource(kafkaSource)
      }
      case _ => throw new IllegalArgumentException("No input media specified")
    }).map(line => {
      val fields = line.split(",")
      // TODO: - 1390209860L ???
      //      EvaluationRequest(fields(2).toInt, fields(3).toInt, fields(0).toLong, 1.0, fields(1).toLong - 1390209860L)
      EvaluationRequest(fields(2).toInt, fields(3).toInt, fields(0).toLong, 1.0, fields(1).toLong)
      //          EvaluationRequest(fields(1).toInt, fields(2).toInt, IDGenerator.next, 1.0, fields(0).toLong)
    })

  private def createOutput(accumulatedResults: DataStream[AccumulatedNdcgResult], ioProperties: GeneralIoProperties): Unit = {
    accumulatedResults
      .map(_.timeSlot)
      .print()

    accumulatedResults
      .writeAsText(ioProperties.outputFile.get, FileSystem.WriteMode.OVERWRITE).setParallelism(1)
  }


  /** Parses all properties from file
    *
    * @param args CLI parameter which contains the properties file
    * @return general IO and MF properties object
    */
  def parseProperties(args: Array[String]): (GeneralIoProperties, GeneralMfProperties, ParameterTool) =
    Utils.getProperties(args) match {
      case Some(properties) =>
        (parseGeneralIoProperties(properties), parseGeneralMfProperties(properties), properties)
      case None => throw new IllegalArgumentException("No properties file found")
    }

  /** Parses general IO properties
    *
    * @param properties
    * @return Algorithm independent specific property object
    */
  def parseGeneralIoProperties(properties: ParameterTool): GeneralIoProperties = {
    val parallelism = properties.getInt("parallelism", 1)

    //
    val inputMedia = Option(properties.get("inputMedia"))
    val inputKafkaHostPort =  Option(properties.get("inputKafkaHostPort"))
    val inputKafkaTopic = Option(properties.get("inputKafkaTopic"))
    val inputFile = Option(properties.get("inputFile"))

    val outputFile = Option(properties.get("outputFile"))

    // period of final NDCG evaluation
    val snapshotLength = properties.getInt("snapshotLength", 86400)

    val withDataStreamLogger = properties.getBoolean("withDataStreamLogger", false)
    val logBackend = properties.get("logBackend", "console")

    val K = properties.getInt("K", 4)
    val doEvalAndWrite = properties.getBoolean("doEvalAndWrite", true)


    GeneralIoProperties(parallelism, inputFile, outputFile, snapshotLength, withDataStreamLogger, logBackend,
      K, doEvalAndWrite, inputMedia, inputKafkaHostPort, inputKafkaTopic)
  }

  /** Parses general MF properties
    *
    * @param properties
    * @return MF specific property object
    */
  def parseGeneralMfProperties(properties: ParameterTool): GeneralMfProperties = {
    val learningRate = properties.getDouble("learningRate")
    val lambda = properties.getDouble("lambda")
    val normalizationThreshold = properties.getDouble("normalizationThreshold")
    val negativeSampleRate = properties.getInt("negativeSampleRate")
    val numFactors = properties.getInt("numFactors")
    val rangeMin = properties.getDouble("randomInitRangeMin")
    val rangeMax = properties.getDouble("randomInitRangeMax")
    val workerK = properties.getInt("workerK")
    val bucketSize = properties.getInt("bucketSize")
    val memorySize = properties.getInt("memorySize", 0)

    GeneralMfProperties(learningRate, lambda, normalizationThreshold, numFactors, negativeSampleRate, rangeMin, rangeMax, workerK, bucketSize, memorySize)
  }

  def runMf(ioProperties: GeneralIoProperties, mfProperties: GeneralMfProperties, parameterTool: ParameterTool)
  : JobExecutionResult = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ioProperties.parallelism)


    val psBuilder = (input: DataStream[EvaluationRequest], env: StreamExecutionEnvironment) =>
      PsResultProcessUtils.mergeLocalTopKs(buildPs(input, mfProperties, ioProperties, parameterTool, env),
        ioProperties.K, ioProperties.parallelism, mfProperties.memorySize)

    val psOutput =  if (ioProperties.withDataStreamLogger) {
      JobLogger.doWithLogging[EvaluationRequest, Recommendation](readInput(env, ioProperties),
//        DbWriterFactory.createDbWriter(ioProperties.logBackend, parameterTool), env, "native",
        DbWriterFactory.createDbWriter(ioProperties.logBackend, parameterTool), env, parameterTool.get("psImplType"),
        psBuilder, _.evaluationId, _.evaluationId
      )
    } else {
      psBuilder(readInput(env, ioProperties), env)
    }

    if(ioProperties.doEvalAndWrite) {
      val accumulatedResults = PsResultProcessUtils.processGlobalTopKs(psOutput, ioProperties)
      createOutput(accumulatedResults, ioProperties)
    }

    env.execute()
  }

}
