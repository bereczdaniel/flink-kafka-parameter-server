package parameter.server.algorithms

import parameter.server.utils.Types.ItemId

object Metrics {

  def ndcg(topK: List[ItemId], targetItemId: ItemId): Double = {
    val position = topK.indexOf(targetItemId)+1
    1 / (log2(position) + 1)
  }

  def log2(x: Double): Double =
    math.log(x) / math.log(2)

}
