package parameter.server.algorithms.matrix.factorization.impl

import initializer.RangedRandomFactorInitializerDescriptor
import types.Vector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.algorithms.matrix.factorization.impl.worker.{MfWorkerLogic, RedisMfWorkerToServerSink}
import parameter.server.algorithms.matrix.factorization.{GeneralMfProperties, MessageParsers, MfPsFactory}
import parameter.server.utils.connectors.redis.RedisPubSubSource
import parameter.server.{ParameterServerSkeleton, WorkerOnlyParameterServer}


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