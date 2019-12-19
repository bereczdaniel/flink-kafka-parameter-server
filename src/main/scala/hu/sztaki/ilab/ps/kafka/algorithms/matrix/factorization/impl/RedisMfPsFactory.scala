package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl

import hu.sztaki.ilab.ps.common.types.GeneralMfProperties
import hu.sztaki.ilab.ps.common.types.RecSysMessages.EvaluationRequest
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.worker.{MfWorkerLogic, RedisMfWorkerToServerSink}
import hu.sztaki.ilab.ps.kafka.{ParameterServerSkeleton, WorkerOnlyParameterServer}
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.{MessageParsers, MfPsFactory}
import hu.sztaki.ilab.ps.kafka.utils.connectors.redis.RedisPubSubSource
import matrix.factorization.initializer.RangedRandomFactorInitializerDescriptor
import matrix.factorization.types.Vector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


class RedisMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton[EvaluationRequest] = {
    val hostName = parameters.get("redis.host")
    val port = parameters.get("redis.port").toInt
    val channelName = parameters.get("redis.channelName")

    new WorkerOnlyParameterServer[EvaluationRequest, Vector, Long, Int](
      env,
      inputStream = inputStream,
      workerLogic = new MfWorkerLogic(generalMfProperties.numFactors, generalMfProperties.learningRate,
        generalMfProperties.negativeSampleRate, generalMfProperties.randomInitRangeMin, generalMfProperties.randomInitRangeMax, generalMfProperties.workerK,
        generalMfProperties.bucketSize),
      serverToWorkerSource = new RedisPubSubSource(hostName, port, channelName),
      serverToWorkerParse = MessageParsers.pullAnswerFromString,
      workerToServerSink = new RedisMfWorkerToServerSink(host = hostName, port = port, channelName = channelName, generalMfProperties.numFactors, generalMfProperties.randomInitRangeMin, generalMfProperties.randomInitRangeMax)
    )
  }
}