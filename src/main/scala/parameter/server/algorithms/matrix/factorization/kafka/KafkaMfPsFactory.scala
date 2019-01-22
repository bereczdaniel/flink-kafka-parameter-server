package parameter.server.algorithms.matrix.factorization.kafka

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import parameter.server.ParameterServerSkeleton
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.algorithms.matrix.factorization.kafka.server.StateBackedMfServerLogic
import parameter.server.algorithms.matrix.factorization.kafka.worker.MfWorkerLogic
import parameter.server.algorithms.matrix.factorization.{GeneralMfProperties, MfPsFactory}
import parameter.server.kafka.{KafkaPsFactory, ParameterServer}
import parameter.server.utils.Vector

class KafkaMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton = {
    val hostName = parameters.get("kafka.host")
    val port = parameters.get("kafka.port").toInt
    val serverToWorkerTopic = parameters.get("kafka.serverToWorkerTopic")
    val workerToServerTopic = parameters.get("kafka.workerToServerTopic")

    val broadcastServerToWorkers = parameters.get("broadcast").contains("y")

    new KafkaPsFactory[EvaluationRequest, Vector, Long, Int].createPs(
      env,
      inputStream = inputStream,
      workerLogic = new MfWorkerLogic(generalMfProperties.numFactors, generalMfProperties.learningRate,
        generalMfProperties.negativeSampleRate, generalMfProperties.rangeMin, generalMfProperties.rangeMax, generalMfProperties.workerK,
        generalMfProperties.bucketSize),
      serverLogic = new StateBackedMfServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum),
      serverToWorkerParse = ParameterServerSkeleton.pullAnswerFromString, workerToServerParse = ParameterServerSkeleton.workerToServerParse,
      host = hostName, port = port, serverToWorkerTopic, workerToServerTopic,
      broadcastServerToWorkers = broadcastServerToWorkers)
  }

}