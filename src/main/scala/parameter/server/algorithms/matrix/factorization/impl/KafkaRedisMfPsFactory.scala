package parameter.server.algorithms.matrix.factorization.impl

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import parameter.server.{KafkaPsFactory, ParameterServerSkeleton}
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.algorithms.matrix.factorization.impl.server.RedisBackedMfServerLogic
import parameter.server.algorithms.matrix.factorization.impl.worker.MfWorkerLogic
import parameter.server.algorithms.matrix.factorization.{GeneralMfProperties, MessageParsers, MfPsFactory}
import parameter.server.utils.Vector


class KafkaRedisMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton[EvaluationRequest] = {
    // kafka parameters
    val kafkaHostName = parameters.get("kafka.host")
    val kafkaPort = parameters.get("kafka.port").toInt
    val serverToWorkerTopic = parameters.get("kafka.serverToWorkerTopic")
    val workerToServerTopic = parameters.get("kafka.workerToServerTopic")

    val broadcastServerToWorkers = parameters.getBoolean("broadcast", true)

    // redis parameters
    val redisHostName = parameters.get("redis.host")
    val redisPort = parameters.get("redis.port").toInt


    new KafkaPsFactory[EvaluationRequest, Vector, Long, Int].createPs(
      env = env,
      inputStream = inputStream,
      workerLogic = new MfWorkerLogic(generalMfProperties.numFactors, generalMfProperties.learningRate,
        generalMfProperties.negativeSampleRate, generalMfProperties.randomInitRangeMin, generalMfProperties.randomInitRangeMax, generalMfProperties.workerK,
        generalMfProperties.bucketSize),
      serverLogic = new RedisBackedMfServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum, redisHostName, redisPort),
      serverToWorkerParse = MessageParsers.pullAnswerFromString, workerToServerParse = MessageParsers.pullOrPushFromString,
      host = kafkaHostName, port = kafkaPort, serverToWorkerTopic = serverToWorkerTopic, workerToServerTopic = workerToServerTopic,
      broadcastServerToWorkers = broadcastServerToWorkers)
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
//    val ps = new WorkerOnlyParameterServer[EvaluationRequest, Vector, Long, Int](
//      env,
//      inputStream = source,
//      workerLogic = new MfWorkerLogic(n, learningRate, 9, -0.001, 0.001, 100, 75),
//      serverLogic = new StateBackedMfServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum),
//      serverToWorkerParse = pullAnswerFromString, pullOrPushFromString = pullOrPushFromString,
//      host = "localhost:", port = 9092, serverToWorkerTopic = "serverToWorkerTopic", workerToServerTopic = "workerToServerTopic",
//      broadcastServerToWorkers = true)
//
//
