package parameter.server.algorithms.matrix.factorization.kafka

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.AbstractOnlineTrainAndEval
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.kafka.ParameterServer
import parameter.server.utils.{Types, Vector}

class OnlineTrainAndEval extends AbstractOnlineTrainAndEval {

  override def testProcessCategory: String = "kafka"

  override def createPS(learningRate: Double, numFactors: Int, negativeSampleRate: Int, rangeMin: Double, rangeMax: Double,
                        workerK: Int, bucketSize: Int,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                         inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): Types.ParameterServerSkeleton = {
    val hostName = parameters.get("host")
    val port = parameters.get("port").toInt
    val serverToWorkerTopic = parameters.get("serverToWorkerTopic")
    val workerToServerTopic = parameters.get("workerToServerTopic")

    val broadcastServerToWorkers = parameters.get("broadcast").contains("y")

    new ParameterServer[EvaluationRequest, Vector, Long, Int](
      env,
      inputStream = inputStream,
      workerLogic = new TrainAndEvalWorkerLogic(numFactors, learningRate, negativeSampleRate, rangeMin, rangeMax, workerK, bucketSize),
      serverLogic = new TrainAndEvalServerLogic(x => Vector(factorInitDesc.open().nextFactor(x.hashCode())), Vector.vectorSum),
      serverToWorkerParse = pullAnswerFromString, workerToServerParse = workerToServerParse,
      host = hostName, port = port, serverToWorkerTopic, workerToServerTopic,
      broadcastServerToWorkers = broadcastServerToWorkers)
  }

}

object OnlineTrainAndEval {
  def main(args: Array[String]): Unit = (new OnlineTrainAndEval).parameterParseAndRun(args)
}