package parameter.server.algorithms.matrix.factorization.redis

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.{GeneralMfProperties, MfPsFactory}
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.redis.ParameterServer
import parameter.server.utils.Types.ParameterServerSkeleton
import parameter.server.utils.{RedisPubSubSource, Vector}


class RedisMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton = {
    val hostName = parameters.get("redis.host")
    val port = parameters.get("redis.port").toInt
    val channelName = parameters.get("redis.channelName")

    new ParameterServer[EvaluationRequest, Vector, Long, Int](
      env,
      src = inputStream,
      workerLogic = new TrainAndEvalWorkerLogic(generalMfProperties.numFactors, generalMfProperties.learningRate,
        generalMfProperties.negativeSampleRate, generalMfProperties.rangeMin, generalMfProperties.rangeMax, generalMfProperties.workerK,
        generalMfProperties.bucketSize, host = hostName, port = port, channelName = channelName),
      serverPubSubSource = new RedisPubSubSource(hostName, port, channelName),
      serverToWorkerParse = ParameterServerSkeleton.pullAnswerFromString
    )
  }

}


//object OnlineTrainAndEval {
//
//  def main(args: Array[String]): Unit = {
//    val K = 100
//    val n = 10
//    val learningRate = 0.4
//    val parallelism = 4
//
//    val redisHost = "localhost"
//    val redisPort = 6379
//    val redisChannel = "uservectors"
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(parallelism)
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
//      workerLogic = new TrainAndEvalWorkerLogic(n, learningRate, 9, -0.001, 0.001, 100, 75, host=redisHost, port=redisPort, channelName=redisChannel),
//      serverPubSubSource = new RedisPubSubSource(host=redisHost, port=redisPort, channelName=redisChannel),
//      serverToWorkerParse = pullAnswerFromString
//      )
