package hu.sztaki.ilab.ps.common.types

case class AccumulatedNdcgResult(nDCG: Double, timeSlot: Long, count: Int) {
  override def toString: String =
    s"$timeSlot,$nDCG,$count"
}
