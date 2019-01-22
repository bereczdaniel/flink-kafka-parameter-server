package parameter.server.algorithms.matrix.factorization.impl

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.algorithms.matrix.factorization.impl.worker.{MfWorkerLogic, RedisMfWorkerToServerSink}
import parameter.server.algorithms.matrix.factorization.{GeneralMfProperties, MessageParsers, MfPsFactory}
import parameter.server.utils.Vector
import parameter.server.utils.connectors.redis.RedisPubSubSource
import parameter.server.{ParameterServerSkeleton, WorkerOnlyParameterServer}


class RedisMfPsFactory extends MfPsFactory {

  override def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton[EvaluationRequest] = {
    val hostName = parameters.get("dbms.host")
    val port = parameters.get("dbms.port").toInt
    val channelName = parameters.get("dbms.channelName")

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
//    val ps = new WorkerOnlyParameterServer[EvaluationRequest, Vector, Long, Int](
//      env,
//      inputStream = source,
//      workerLogic = new MfWorkerLogic(n, learningRate, 9, -0.001, 0.001, 100, 75, host=redisHost, port=redisPort, channelName=redisChannel),
//      serverToWorkerSource = new RedisPubSubSource(host=redisHost, port=redisPort, channelName=redisChannel),
//      serverToWorkerParse = pullAnswerFromString
//      )
