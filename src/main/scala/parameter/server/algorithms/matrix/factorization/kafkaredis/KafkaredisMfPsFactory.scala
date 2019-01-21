package parameter.server.algorithms.matrix.factorization.kafkaredis

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.{GeneralMfProperties, MfPsFactory}
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.kafkaredis.ParameterServer
import parameter.server.utils.Types.ParameterServerSkeleton
import parameter.server.utils.{Types, Vector}


class KafkaredisMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): Types.ParameterServerSkeleton = {
    // kafka parameter
    val kafkaHostName = parameters.get("kafka.host")
    val kafkaPort = parameters.get("kafka.port").toInt
    val serverToWorkerTopic = parameters.get("kafka.serverToWorkerTopic")
    val workerToServerTopic = parameters.get("kafka.workerToServerTopic")

    val broadcastServerToWorkers = parameters.getBoolean("broadcast", true)

    // redis parameter
    val redisHostName = parameters.get("redis.host")
    val redisPort = parameters.get("redis.port").toInt


    new ParameterServer[EvaluationRequest, Vector, Long, Int](
      env,
      src = inputStream,
      workerLogic = new TrainAndEvalWorkerLogic(generalMfProperties.numFactors, generalMfProperties.learningRate,
        generalMfProperties.negativeSampleRate, generalMfProperties.rangeMin, generalMfProperties.rangeMax, generalMfProperties.workerK,
        generalMfProperties.bucketSize),
      serverLogic = new TrainAndEvalServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum, redisHostName, redisPort),
      serverToWorkerParse = ParameterServerSkeleton.pullAnswerFromString, workerToServerParse = ParameterServerSkeleton.workerToServerParse,
      kafkaHostName, kafkaPort, serverToWorkerTopic, workerToServerTopic,
      broadcastServerToWorkers)
  }

}
//object OnlineTrainAndEval {
//
//  def main(args: Array[String]): Unit = {
//    val K = 100
//    val n = 10
//    val learningRate = 0.4
//    val parallelism = 4
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(parallelism)
//
//
//    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(n, -0.001, 0.001)
//
//    val source = env
//      .readTextFile("data/recsys-2017-tutorial/1000_test_batch.csv")
//      .map(line => {
//        val fields = line.split(",")
//        EvaluationRequest(fields(1).toInt, fields(2).toInt, IDGenerator.next, 1.0, fields(0).toLong)
//      })
//
//    val ps = new ParameterServer[EvaluationRequest, Vector, Long, Int](
//      env,
//      src = source,
//      workerLogic = new TrainAndEvalWorkerLogic(n, learningRate, 9, -0.001, 0.001, 100, 75),
//      serverLogic = new TrainAndEvalServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum),
//      serverToWorkerParse = pullAnswerFromString, workerToServerParse = workerToServerParse,
//      host = "localhost:", port = 9092, serverToWorkerTopic = "serverToWorkerTopic", workerToServerTopic = "workerToServerTopic",
//      broadcastServerToWorkers = true)
//
//
