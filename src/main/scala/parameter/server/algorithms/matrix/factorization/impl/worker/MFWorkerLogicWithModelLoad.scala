package parameter.server.algorithms.matrix.factorization.impl.worker

import org.apache.flink.util.Collector
import parameter.server.algorithms.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import parameter.server.algorithms.matrix.factorization.RecSysMessages._
import parameter.server.algorithms.matrix.factorization.impl.ItemModel
import parameter.server.algorithms.pruning.{LEMPPruningStrategy, LI}
import parameter.server.communication.Messages
import parameter.server.communication.Messages.{Pull, Push}
import parameter.server.logic.worker.WorkerLogic
import parameter.server.utils.{Types, Vector}

import scala.collection.mutable

class MFWorkerLogicWithModelLoad(numFactors: Int, learningRate: Double, negativeSampleRate: Int,
                                 rangeMin: Double, rangeMax: Double,
                                 workerK: Int, bucketSize: Int, pruningStrategy: LEMPPruningStrategy = LI(5, 2.5))
  extends WorkerLogic[Long, Int, Temp, Vector] {

  lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  lazy val SGDUpdater = new SGDUpdater(learningRate)

  val model = new ItemModel(learningRate, negativeSampleRate, numFactors, rangeMin, rangeMax, bucketSize, workerK, pruningStrategy)

  val requestBuffer = new mutable.HashMap[Long, EvaluationRequest]()


  override def onPullReceive(msg: Messages.Message[Int, Long, Vector],
                             out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Long, Int, Vector]]]): Unit = {
    //logger.info("User vector received by worker from server.")
    val userVector = msg.message.get

    val topK = model.predict(userVector).toList

    val _request = requestBuffer.get(msg.destination)

    _request match {
      case None =>
        // when the observation which initiated the db query belongs to another worker - output a local topK for the observation with dummy additional data:
        out.collect(Left(EvaluationOutput(-1, msg.destination, topK, -1)))

      case Some(request) =>
        // when the observation which initiated the db query belongs to this worker - output the local topK with the request data (itemId & ts):

        val userDelta: Vector = model.train(userVector, request.itemId, request.rating)

        // update user vector on server with userDelta
        out.collect(Right(Push(msg.destination, msg.source, userDelta)))
        //// in redis:
        //pushClient.evalSHA(pushScriptId.get, List(msg.source), userDelta.value.toList)

        out.collect(Left(EvaluationOutput(request.itemId, request.evaluationId, topK, request.ts)))
    }
  }

  override def onInputReceive(data: Temp,
                              out: Collector[Either[Types.ParameterServerOutput, Messages.Message[Long, Int, Vector]]]): Unit = {

    data.data match {
      case Left(evaluationRequest) =>
        requestBuffer.update(evaluationRequest.evaluationId, evaluationRequest)
        out.collect(Right(Pull(evaluationRequest.evaluationId, evaluationRequest.userId)))

      case Right(modelParameter) =>
        modelParameter match {
          case ItemParameter(itemId, parameter) => model.set(itemId, parameter)
          case UserParameter(userId, parameter) => out.collect(Right(Push(-1, userId, parameter)))
        }
    }
  }
}
