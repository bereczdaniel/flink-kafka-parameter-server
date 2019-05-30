package parameter.server.logic.worker

import model.ModelState
import parameter.server.utils.Types.WorkerInput
import types.Parameter

abstract class WorkerLogicWithModel[WK, SK, T <: WorkerInput, P <: Parameter]
  extends WorkerLogic[WK, SK, T, P] {
  val model: ModelState[WK, P]


}
