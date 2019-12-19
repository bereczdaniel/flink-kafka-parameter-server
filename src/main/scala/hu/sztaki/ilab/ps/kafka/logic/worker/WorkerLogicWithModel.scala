package hu.sztaki.ilab.ps.kafka.logic.worker

import matrix.factorization.model.ModelState
import parameter.server.utils.Types.WorkerInput
import matrix.factorization.types.Parameter

abstract class WorkerLogicWithModel[WK, SK, T <: WorkerInput, P <: Parameter]
  extends WorkerLogic[WK, SK, T, P] {
  val model: ModelState[WK, P]


}
