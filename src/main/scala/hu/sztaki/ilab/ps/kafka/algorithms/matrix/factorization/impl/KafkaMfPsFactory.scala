package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl

import hu.sztaki.ilab.ps.common.types.GeneralMfProperties
import hu.sztaki.ilab.ps.common.types.RecSysMessages.EvaluationRequest
import hu.sztaki.ilab.ps.kafka.{KafkaPsFactory, ParameterServerSkeleton}
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.{MessageParsers, MfPsFactory}
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.server.StateBackedMfServerLogic
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.worker.MfWorkerLogic
import matrix.factorization.initializer.RangedRandomFactorInitializerDescriptor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import matrix.factorization.types.Vector
import org.apache.flink.streaming.api.scala._

class KafkaMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton[EvaluationRequest] = {
    val hostName = parameters.get("kafka.host")
    val port = parameters.get("kafka.port").toInt
    val serverToWorkerTopic = parameters.get("kafka.serverToWorkerTopic")
    val workerToServerTopic = parameters.get("kafka.workerToServerTopic")

    val broadcastServerToWorkers = parameters.getBoolean("broadcast")

    new KafkaPsFactory[EvaluationRequest, Vector, Long, Int].createPs(
      env = env,
      inputStream = inputStream,
      workerLogic = new MfWorkerLogic(generalMfProperties.numFactors, generalMfProperties.learningRate,
        generalMfProperties.negativeSampleRate, generalMfProperties.randomInitRangeMin, generalMfProperties.randomInitRangeMax, generalMfProperties.workerK,
        generalMfProperties.bucketSize),
      serverLogic = new StateBackedMfServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum),
      serverToWorkerParse = MessageParsers.pullAnswerFromString, workerToServerParse = MessageParsers.pullOrPushFromString,
      host = hostName, port = port, serverToWorkerTopic = serverToWorkerTopic, workerToServerTopic = workerToServerTopic,
      broadcastServerToWorkers = broadcastServerToWorkers)
  }

}