package parameter.server.algorithms.matrix.factorization.redis

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import parameter.server.algorithms.Metrics
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.RecSysMessages.{EvaluationOutput, EvaluationRequest}
import parameter.server.communication.Messages._
import parameter.server.redis.ParameterServer
import parameter.server.utils.Types.{ItemId, ParameterServerOutput}
import parameter.server.utils.{IDGenerator, RedisPubSubSource, Vector}

object OnlineTrainAndEval {

  def main(args: Array[String]): Unit = {
    val K = 100
    val n = 10
    val learningRate = 0.4
    val parallelism = 4

    val redisHost = "localhost"
    val redisPort = 6379
    val redisChannel = "uservectors"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(n, -0.001, 0.001)

    val source = env
      .readTextFile("data/recsys-2017-tutorial/1000_test_batch.csv")
      .map(line => {
        val fields = line.split(",")
        EvaluationRequest(fields(1).toInt, fields(2).toInt, IDGenerator.next, 1.0, fields(0).toLong)
      })

    val ps = new ParameterServer[EvaluationRequest, Vector, Long, Int](
      env,
      src = source,
      workerLogic = new TrainAndEvalWorkerLogic(n, learningRate, 9, -0.001, 0.001, 100, 75, host=redisHost, port=redisPort, channelName=redisChannel),
      serverPubSubSource = new RedisPubSubSource(host=redisHost, port=redisPort, channelName=redisChannel),
      serverToWorkerParse = pullAnswerFromString
      )

    val topKOut = ps
      .start()
        .flatMap(new FlatMapFunction[ParameterServerOutput, EvaluationOutput] {
          override def flatMap(value: ParameterServerOutput, out: Collector[EvaluationOutput]): Unit = {
            value match {
              case eval: EvaluationOutput => out.collect(eval)
              case _ => throw new NotSupportedOutput
            }
          }
        })


    val mergedTopK = topKOut
      .keyBy(_.evaluationId)
      .flatMapWithState((localTopK: EvaluationOutput, aggregatedTopKs: Option[List[EvaluationOutput]]) => {
        aggregatedTopKs match {
          case None =>
            (List.empty, Some(List(localTopK)))
          case Some(currentState) =>
            if(currentState.length < parallelism-1) {
              (List.empty, Some(currentState.++:(List(localTopK))))
            }
            else {
              val allTopK = currentState.++(List(localTopK))
              val topK = allTopK.map(_.topK).fold(List[(ItemId, Double)]())((a, b) => a ++ b).distinct.sortBy(-_._2).map(_._1).take(K)
              val targetItemId = allTopK.maxBy(_.itemId).itemId
              val ts = allTopK.maxBy(_.ts).ts
              val nDCG = Metrics.ndcg(topK, targetItemId)
              (List((localTopK.evaluationId, nDCG, ts)), None)
            }
        }
      })
      .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .process(new ProcessAllWindowFunction[(Long, Double, Long), (Long, Double), TimeWindow] {
        override def process(context: Context, elements: Iterable[(Long, Double, Long)], out: Collector[(Long, Double)]): Unit = {
          val grouped = elements
            .groupBy(x => x._3 / 86400)

          grouped
            .foreach(x => out.collect((x._1, x._2.map(_._2).sum / x._2.size)))
          out.collect((0L, elements.map(_._2).sum / elements.size))
        }
      })


    mergedTopK
        .writeAsText("data/output/10_nDCG_synchronous", FileSystem.WriteMode.OVERWRITE).setParallelism(1)


    env.execute()

  }

  def pullAnswerFromString(line: String): PullAnswer[Long, Int, Vector] = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toLong, Vector(fields(2).split(",").map(_.toDouble)))
  }
}
