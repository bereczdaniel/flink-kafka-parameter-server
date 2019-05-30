package parameter.server.algorithms.matrix.factorization.impl.worker

import LEMP.{LI, PruningStrategy}
import model.ItemModel
import types.Vector
import org.apache.flink.util.Collector
import parameter.server.algorithms.matrix.factorization.RecSysMessages.{EvaluationOutput, EvaluationRequest}
import parameter.server.communication.Messages
import parameter.server.communication.Messages.{Pull, Push}
import parameter.server.logic.worker.WorkerLogic
import parameter.server.utils.Types.ParameterServerOutput

import scala.collection.mutable

class MfWorkerLogic(numFactors: Int, learningRate: Double, negativeSampleRate: Int,
                    rangeMin: Double, rangeMax: Double,
                    workerK: Int, bucketSize: Int, pruningStrategy: PruningStrategy = LI(5, 2.5))
  extends WorkerLogic[Long, Int, EvaluationRequest, Vector]{

  lazy val model = new ItemModel(learningRate, negativeSampleRate, numFactors, rangeMin, rangeMax, bucketSize, workerK, pruningStrategy)

  lazy val requestBuffer = new mutable.HashMap[Long, EvaluationRequest]()



  override def onPullReceive(msg: Messages.Message[Int, Long, Vector],
                             out: Collector[Either[ParameterServerOutput, Messages.Message[Long, Int, Vector]]]): Unit = {
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

  override def onInputReceive(data: EvaluationRequest,
                              out: Collector[Either[ParameterServerOutput, Messages.Message[Long, Int, Vector]]]): Unit = {
    //logger.info("Input received by worker.")
    requestBuffer.update(data.evaluationId, data)

    // Query user vector from server (it will then send to the server-to-worker channel for broadcasting):
    out.collect(Right(Pull(data.evaluationId, data.userId)))
    //// in redis:
    //redisClient.evalSHA(pullScriptId.get, List(data.userId),
    //  List(data.evaluationId, channelName, numFactors, randomInitRangeMin, randomInitRangeMax))

  }
}
