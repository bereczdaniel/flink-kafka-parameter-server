package parameter.server.algorithms.matrix.factorization.impl

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import parameter.server.{KafkaPsFactory, ParameterServerSkeleton}
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.algorithms.matrix.factorization.impl.server.StateBackedMfServerLogic
import parameter.server.algorithms.matrix.factorization.impl.worker.MfWorkerLogic
import parameter.server.algorithms.matrix.factorization.{GeneralMfProperties, MessageParsers, MfPsFactory}
import parameter.server.utils.Types.WorkerInput
import parameter.server.utils.Vector

class KafkaMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton[EvaluationRequest] = {
    val hostName = parameters.get("kafka.host")
    val port = parameters.get("kafka.port").toInt
    val serverToWorkerTopic = parameters.get("kafka.serverToWorkerTopic")
    val workerToServerTopic = parameters.get("kafka.workerToServerTopic")

    val broadcastServerToWorkers = parameters.get("broadcast").contains("y")

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