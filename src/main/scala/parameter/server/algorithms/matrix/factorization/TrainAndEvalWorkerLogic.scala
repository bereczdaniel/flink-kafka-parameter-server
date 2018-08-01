package parameter.server.algorithms.matrix.factorization

import org.apache.flink.util.Collector
import parameter.server.algorithms.matrix.factorization.RecSysMessages.{EvaluationOutput, EvaluationRequest}
import parameter.server.communication.Messages
import parameter.server.communication.Messages.Pull
import parameter.server.logic.worker.WorkerLogic
import parameter.server.utils.Types.ItemId
import parameter.server.utils.{Types, Vector}

import scala.collection.mutable

class TrainAndEvalWorkerLogic extends WorkerLogic[EvaluationRequest] {
  val model = new mutable.HashMap[ItemId, Vector]()

  val requestBuffer = new mutable.HashMap[AnyVal, EvaluationRequest]()

  override def onPullReceive(pullAnswer: Messages.Message, out: Collector[Either[Types.ParameterServerOutput, Messages.Message]]): Unit = {
    val _request = requestBuffer.get(pullAnswer.destination)

    _request match {
      case None =>
      case Some(evalReq) =>
        println(evalReq)
        out.collect(Left(EvaluationOutput(evalReq.itemId, evalReq.evaluationId, Types.createTopK, evalReq.ts)))
    }
  }

  override def onInputReceive(data: EvaluationRequest, out: Collector[Either[Types.ParameterServerOutput, Messages.Message]]): Unit = {
      requestBuffer.update(data.evaluationId, data)

      out.collect(Right(Pull(workerId, data.userId)))
  }
}

object TrainAndEvalWorkerLogic {
  def apply: TrainAndEvalWorkerLogic = new TrainAndEvalWorkerLogic()
}
