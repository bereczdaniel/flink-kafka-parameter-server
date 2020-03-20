package hu.sztaki.ilab.ps.common

//Naming conflict between packages: matrix.factorization is a subpackage here as well as a root package from an imported module.
//The local subpackage overloads the root package reference. We must use explicit absolute package path starting from _root_:
import _root_.matrix.factorization.types.ItemId

object Metrics {

  /**
    *
    * @param topK
    * @param targetItemId
    * @return nDCG between 0 and 1, 1.0 if targetItemId is the first (leftmost) element of topK,
    *         -0.0 if the topK does not contain targetItemId
    */
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
