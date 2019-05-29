package parameter.server.logic.worker

import parameter.server.algorithms.models.ModelState
import parameter.server.utils.Types.{Parameter, WorkerInput}

abstract class WorkerLogicWithModel[WK, SK, T <: WorkerInput, P <: Parameter]
  extends WorkerLogic[WK, SK, T, P] {
  val model: ModelState[WK, P]


}
