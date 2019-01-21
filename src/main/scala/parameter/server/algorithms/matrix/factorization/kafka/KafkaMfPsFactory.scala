package parameter.server.algorithms.matrix.factorization.kafka

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.{GeneralMfProperties, MfPsFactory}
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.kafka.ParameterServer
import parameter.server.utils.Types.ParameterServerSkeleton
import parameter.server.utils.{Types, Vector}

class KafkaMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): Types.ParameterServerSkeleton = {
    val hostName = parameters.get("kafka.host")
    val port = parameters.get("kafka.port").toInt
    val serverToWorkerTopic = parameters.get("kafka.serverToWorkerTopic")
    val workerToServerTopic = parameters.get("kafka.workerToServerTopic")

    val broadcastServerToWorkers = parameters.get("broadcast").contains("y")

    new ParameterServer[EvaluationRequest, Vector, Long, Int](
      env,
      inputStream = inputStream,
      workerLogic = new TrainAndEvalWorkerLogic(generalMfProperties.numFactors, generalMfProperties.learningRate,
        generalMfProperties.negativeSampleRate, generalMfProperties.rangeMin, generalMfProperties.rangeMax, generalMfProperties.workerK,
        generalMfProperties.bucketSize),
      serverLogic = new TrainAndEvalServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum),
      serverToWorkerParse = ParameterServerSkeleton.pullAnswerFromString, workerToServerParse = ParameterServerSkeleton.workerToServerParse,
      host = hostName, port = port, serverToWorkerTopic, workerToServerTopic,
      broadcastServerToWorkers = broadcastServerToWorkers)
  }

}