package parameter.server.algorithms

import parameter.server.utils.Types.ItemId

object Metrics {

  def ndcg(topK: List[ItemId], targetItemId: ItemId): Double = {
    dcg(topK, List(targetItemId))
  }

  def dcg(topK: List[ItemId], targets: List[ItemId]): Double = {
    (for(p <- 1 to topK.size) yield {
      if(targets.contains(topK(p-1)))
        1 / log2(p+1)
      else
        0
    }).sum
  }

  def log2(x: Double): Double =
    math.log(x) / math.log(2)

}
