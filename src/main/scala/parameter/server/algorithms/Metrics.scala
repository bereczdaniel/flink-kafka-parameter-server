package parameter.server.algorithms

import parameter.server.algorithms.matrix.factorization.Types.ItemId

object Metrics {

  def nDCG(topK: List[ItemId], targetItemId: ItemId): Double = {
    val position = topK.indexOf(targetItemId) + 1
    1 / (log2(position) + 1)
  }

  def log2(x: Double): Double =
    math.log(x) / math.log(2)

  def dcg(topK: List[ItemId], targets: List[ItemId]): Double = {
    (for(p <- 1 to topK.size) yield {
      if(targets.contains(topK(p-1)))
        1 / log2(p+1)
      else
        0
    }).sum
  }

  def IDCG(targets: List[ItemId], k: Int): Double = {
    (for(p <- 1 to math.min(targets.size, k)) yield {
      1 / log2(p+1)
    }).sum
  }

  def nDCG(topK: List[ItemId], targets: List[ItemId]): Double =
    dcg(topK, targets) / IDCG(targets, topK.size)

}
