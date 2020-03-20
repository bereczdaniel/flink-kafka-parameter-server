package hu.sztaki.ilab.ps.common

import hu.sztaki.ilab.ps.common.types.RecSysMessages.EvaluationOutput
import matrix.factorization.types.{ItemId, Prediction, Recommendation, TopK, UserId, Vector}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A flat map function that receives TopKs from each worker, and outputs the overall TopK.
  * Used on data coming out of the parameter server.
  *
  * The Input contains a disjoint union of TopK worker outputs on the Left, and
  * Server outputs on the Right (which will be discarded)
  *
  * The Output contains tuples of user ID, item ID, timestamp, and TopK List
  *
  * @param K                 Number of items in the generated recommendation
  * @param memory            The last #memory item seen by the user will not be recommended
  * @param workerParallelism Number of worker nodes
  */
class CollectTopKFromEachLearnWorker(K: Int, memory: Int, workerParallelism: Int)
  extends RichFlatMapFunction[EvaluationOutput, Recommendation] {

  val topKs = new mutable.HashMap[Long, ArrayBuffer[TopK]]
  val itemIdRelation = new mutable.HashMap[Long, ItemId]
  val tsRelation = new mutable.HashMap[Long, Long]
  val seenSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
  val seenList = new mutable.HashMap[UserId, mutable.Queue[ItemId]]

  override def flatMap(value: EvaluationOutput, out: Collector[Recommendation]): Unit = {

    val allTopK = topKs.getOrElseUpdate(value.evaluationId, new ArrayBuffer[TopK]())

    allTopK += value.topK

    if (value.itemId > -1 && !itemIdRelation.contains(value.evaluationId)) {
      itemIdRelation += value.evaluationId -> value.itemId
    }
    if (value.ts > -1 && !tsRelation.contains(value.evaluationId)) {
      tsRelation += value.evaluationId -> value.ts
    }


    if (allTopKReceived(allTopK)) {
      val topKList = allTopK.fold(new mutable.ListBuffer[Prediction]())((q1, q2) => q1 ++= q2).toList
        .filterNot(x => seenSet.getOrElseUpdate(value.userId, new mutable.HashSet) contains x.itemId)
        .sortBy(-_.score)
        .take(K).map(_.itemId)

      val itemId = itemIdRelation.get(value.evaluationId).get

      out.collect(Recommendation(itemId, topKList, value.evaluationId, tsRelation.get(value.evaluationId).get))
      topKs -= value.evaluationId

      seenSet.getOrElseUpdate(value.userId, new mutable.HashSet) += itemId
      seenList.getOrElseUpdate(value.userId, new mutable.Queue) += itemId
      if ((memory > -1) && (seenList(value.userId).length > memory)) {
        seenSet(value.userId) -= seenList(value.userId).dequeue()
      }
    }
  }

  def allTopKReceived(allTopK: ArrayBuffer[TopK]): Boolean =
    allTopK.size == workerParallelism


}