package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl

import hu.sztaki.ilab.ps.common.types.GeneralMfProperties
import hu.sztaki.ilab.ps.common.types.RecSysMessages.EvaluationRequest
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.server.RedisBackedMfServerLogic
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.worker.MfWorkerLogic
import hu.sztaki.ilab.ps.kafka.{KafkaPsFactory, ParameterServerSkeleton}
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.{MessageParsers, MfPsFactory}
import matrix.factorization.initializer.RangedRandomFactorInitializerDescriptor
import matrix.factorization.types.Vector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


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
      workerLogic = new MfWorkerLogic(generalMfProperties.numFactors, generalMfProperties.learningRate, lambda = generalMfProperties.lambda, normalizationThreshold = generalMfProperties.normalizationThreshold,
        generalMfProperties.negativeSampleRate, generalMfProperties.randomInitRangeMin, generalMfProperties.randomInitRangeMax, generalMfProperties.workerK,
        generalMfProperties.bucketSize),
      serverLogic = new RedisBackedMfServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum, redisHostName, redisPort),
      serverToWorkerParse = MessageParsers.pullAnswerFromString, workerToServerParse = MessageParsers.pullOrPushFromString,
      host = kafkaHostName, port = kafkaPort, serverToWorkerTopic = serverToWorkerTopic, workerToServerTopic = workerToServerTopic,
      broadcastServerToWorkers = broadcastServerToWorkers)
  }
}