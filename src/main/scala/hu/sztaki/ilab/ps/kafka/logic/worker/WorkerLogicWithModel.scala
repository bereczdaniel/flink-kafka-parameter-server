package hu.sztaki.ilab.ps.kafka.logic.worker

import hu.sztaki.ilab.ps.common.types.WorkerInput
import matrix.factorization.model.ModelState
import matrix.factorization.types.Parameter

abstract class WorkerLogicWithModel[WK, SK, T <: WorkerInput, P <: Parameter]
  extends WorkerLogic[WK, SK, T, P] {
  val model: ModelState[WK, P]


}
