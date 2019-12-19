package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.worker

import hu.sztaki.ilab.ps.communication.Messages.Message
import hu.sztaki.ilab.ps.kafka.communication.Messages.Message
import org.apache.flink.util.Collector
import matrix.factorization.types.Vector

class MFWorkerLogicWithModelLoad(wl: MfWorkerLogic)
  extends WorkerLogic[Long, Int, WorkerInput, Vector] {


  override def onPullReceive(msg: Message[Int, Long, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Message[Long, Int, Vector]]]): Unit =
    wl.onPullReceive(msg, out)

  override def onInputReceive(data: WorkerInput,
                              out: Collector[Either[Types.ParameterServerOutput, Message[Long, Int, Vector]]]): Unit = {

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
