package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.worker

import hu.sztaki.ilab.ps.common.types.ParameterServerOutput
import hu.sztaki.ilab.ps.common.types.RecSysMessages.{EvaluationOutput, EvaluationRequest}
import hu.sztaki.ilab.ps.kafka.communication.Messages
import hu.sztaki.ilab.ps.kafka.communication.Messages.{Pull, Push}
import hu.sztaki.ilab.ps.kafka.logic.worker.WorkerLogic
import matrix.factorization.LEMP.{LI, PruningStrategy}
import matrix.factorization.model.ItemModel
import matrix.factorization.types.{Prediction, Vector}
import org.apache.flink.util.Collector

import scala.collection.mutable

class MfWorkerLogic(numFactors: Int, learningRate: Double, lambda: Double, normalizationThreshold: Double,
                    negativeSampleRate: Int, rangeMin: Double, rangeMax: Double,
                    workerK: Int, bucketSize: Int, pruningStrategy: PruningStrategy = LI(5, 2.5))
  extends WorkerLogic[Long, Int, EvaluationRequest, Vector]{

  lazy val model = new ItemModel(learningRate, lambda, normalizationThreshold, negativeSampleRate, numFactors, rangeMin, rangeMax, bucketSize, workerK, pruningStrategy)
//  lazy val model = new ItemModel(learningRate, negativeSampleRate, numFactors, rangeMin, rangeMax, bucketSize, workerK, pruningStrategy)

  lazy val requestBuffer = new mutable.HashMap[Long, EvaluationRequest]()


  private def sendLocalTopK(msg: Messages.Message[Int, Long, Vector], request: Option[EvaluationRequest], userVector: Vector,
                            out: Collector[Either[ParameterServerOutput, Messages.Message[Long, Int, Vector]]]) =
    request match {
    case None if msg.destination > 0 =>
      // when the observation which initiated the db query belongs to another worker - output a local topK for the observation with dummy additional data:
      // msg.destination carries the evaluationId
      out.collect(Left(EvaluationOutput(msg.source, -1, msg.destination, mutable.PriorityQueue[Prediction](model.predict(userVector).toList: _*), -1)))
    case Some(r) if msg.destination > 0 =>
      // when the observation which initiated the db query belongs to this worker - output the local topK with the request data (itemId & ts):
      out.collect(Left(EvaluationOutput(msg.source, r.itemId, r.evaluationId, mutable.PriorityQueue[Prediction](model.predict(userVector).toList: _*), r.ts)))
    case _ =>
      if (msg.destination == 0) {
         out.collect(Left(EvaluationOutput(0, 0, 0, new mutable.PriorityQueue[Prediction](), 0)))
      }


    }


  override def onPullReceive(msg: Messages.Message[Int, Long, Vector],
                             out: Collector[Either[ParameterServerOutput, Messages.Message[Long, Int, Vector]]]): Unit = {
    //logger.info("User vector received by worker from server.")
    val userVector = msg.message.get

    val request = requestBuffer.get(msg.destination)
    sendLocalTopK(msg, request, userVector, out)

    if (request.nonEmpty) {
      val r = request.get
      val userDelta: Vector = model.train(userVector, r.itemId, r.rating)

      // update user vector on server with userDelta
      out.collect(Right(Push(msg.destination, msg.source, userDelta)))
      //// in redis:
      //pushClient.evalSHA(pushScriptId.get, List(msg.source), userDelta.value.toList)
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