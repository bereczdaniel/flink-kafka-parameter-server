package parameter.server.algorithms.matrix.factorization.impl.worker

import org.apache.flink.util.Collector
import parameter.server.algorithms.matrix.factorization.RecSysMessages._
import parameter.server.communication.Messages
import parameter.server.communication.Messages.Push
import parameter.server.logic.worker.WorkerLogic
import parameter.server.utils.Types
import parameter.server.utils.Types.WorkerInput
import types.Vector

class MFWorkerLogicWithModelLoad(wl: MfWorkerLogic)
  extends WorkerLogic[Long, Int, WorkerInput, Vector] {


  override def onPullReceive(msg: Messages.Message[Int, Long, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Long, Int, Vector]]]): Unit =
    wl.onPullReceive(msg, out)

  override def onInputReceive(data: WorkerInput,
                              out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Long, Int, Vector]]]): Unit = {

    data match {
      case e @ EvaluationRequest(_, _, _, _, _) =>
        wl.onInputReceive(e, out)

      case UserParameter(userId, parameter) =>
        out.collect(Right(Push(-1, userId, parameter)))

      case ItemParameter(itemId, param) =>
        wl.model.set(itemId, param)
    }
  }
}
