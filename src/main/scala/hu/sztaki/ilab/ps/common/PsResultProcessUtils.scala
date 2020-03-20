package hu.sztaki.ilab.ps.common

import hu.sztaki.ilab.ps.common.types.RecSysMessages.EvaluationOutput
import hu.sztaki.ilab.ps.common.types.{AccumulatedNdcgResult, GeneralIoProperties, NdcgResult, NotSupportedOutput, ParameterServerOutput}
import matrix.factorization.types.{Recommendation, UserId}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PsResultProcessUtils {

  def mergeLocalTopKs(ps: DataStream[_ <:ParameterServerOutput], K: Int, parallelism: Int, memorySize: Int): DataStream[Recommendation] =
    merge(ps
      .flatMap(_ match {
        case eval: EvaluationOutput => Some(eval)
        case _ => throw new NotSupportedOutput
      }), K, parallelism, memorySize)

//  private def merge(psOut: DataStream[EvaluationOutput], K: Int, parallelism: Int): DataStream[Recommendation] =
//    psOut
//      .keyBy(_.evaluationId)
//      .countWindow(parallelism)
//      .process(new ProcessWindowFunction[EvaluationOutput, Recommendation, Long, GlobalWindow] {
//        override def process(key: Long, context: Context,
//                             elements: Iterable[EvaluationOutput],
//                             out: Collector[Recommendation]): Unit = {
//
//          out.collect(mergeLogic(elements, K))
//        }
//      })

  // TODO: shophisticated solution needed? Consider ValueState?
  private def merge(psOut: DataStream[EvaluationOutput], K: Int, parallelism: Int, memorySize: Int): DataStream[Recommendation] =
    psOut.partitionCustom(new Partitioner[Int] {
      override def partition(key: Int, numPartitions: Int): UserId = { key % numPartitions }
    }, x => x.userId).flatMap(new CollectTopKFromEachLearnWorker(K, memorySize, parallelism))



//  private def mergeLogic(elements: Iterable[EvaluationOutput], K: Int): Recommendation = {
//    val target = elements.head.itemId
//    // TODO check which is correct? .sortBy(- _.score).take(K)
////    val topK = elements.flatMap(_.topK).toList.sortBy(_.score).takeRight(K).map(_.itemId)
//    val topK = elements.flatMap(_.topK).toList.sortBy(- _.score).take(K).map(_.itemId)
//    val id = elements.head.evaluationId
//    val ts = elements.head.ts
//    Recommendation(target, topK, id, ts)
//  }

  def processGlobalTopKs(recommendations: DataStream[Recommendation], ioProperties: GeneralIoProperties)
  : DataStream[AccumulatedNdcgResult] =
    accumulateNdcgs(calculateNdcgs(recommendations), ioProperties.snapshotLength)

  private def calculateNdcgs(recommendations: DataStream[Recommendation]): DataStream[NdcgResult] =
    recommendations
      .map(rec => {
        val nDCG = Metrics.nDCG(rec.topK, rec.targetId)
        NdcgResult(rec.evaluationId, nDCG, rec.timestamp)
      })

  private def accumulateNdcgs(NdcgResults: DataStream[NdcgResult], snapshotLength: Long): DataStream[AccumulatedNdcgResult] =
    NdcgResults
      .keyBy(r => r.timestamp / snapshotLength)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(60)))
      .process(new ProcessWindowFunction[NdcgResult, AccumulatedNdcgResult, Long, TimeWindow] {
        override def process(key: Long, context: Context,
                             elements: Iterable[NdcgResult],
                             out: Collector[AccumulatedNdcgResult]): Unit = {
          val count = elements.size
          val avg_nDCG: Double = elements.map(_.nDCG).sum / count

          out.collect(AccumulatedNdcgResult(avg_nDCG, key, count))
        }
      })
}
