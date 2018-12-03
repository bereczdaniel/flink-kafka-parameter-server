package parameter.server.algorithms.matrix.factorization.redis

import java.io.InputStream

import com.redis.RedisClient
import grizzled.slf4j.Logging
import org.apache.flink.util.Collector
import parameter.server.algorithms.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import parameter.server.algorithms.matrix.factorization.RecSysMessages.{EvaluationOutput, EvaluationRequest}
import parameter.server.algorithms.pruning.LEMPPruningFunctions._
import parameter.server.algorithms.pruning._
import parameter.server.communication.Messages
import parameter.server.redis.logic.worker.WorkerLogic
import parameter.server.utils.Types.ItemId
import parameter.server.utils.{Types, Vector}

import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks._

class TrainAndEvalWorkerLogic(numFactors: Int, learningRate: Double, negativeSampleRate: Int,
                              rangeMin: Double, rangeMax: Double,
                              workerK: Int, bucketSize: Int, pruningStrategy: LEMPPruningStrategy = LI(5, 2.5),
                              host: String, port: Int, channelName: String)
  extends WorkerLogic[Long, Int, EvaluationRequest, Vector] with Logging {

  lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  lazy val SGDUpdater = new SGDUpdater(learningRate)

  val model = new mutable.HashMap[ItemId, Vector]()
  lazy val pushClient = new RedisClient(host, port)
  lazy val pullClient = new RedisClient(host, port)
  lazy val pullScriptId = pullClient.scriptLoad(loadScriptContent("/scripts/redis/pull_user_vector.lua"))
  lazy val pushScriptId = pushClient.scriptLoad(loadScriptContent("/scripts/redis/push_update_user_vector.lua"))

  def itemIds: Array[ItemId] = model.keySet.toArray
  val itemIdsDescendingByLength = new mutable.TreeSet[(ItemId, Double)]()(Types.topKOrdering)

  val requestBuffer = new mutable.HashMap[Long, EvaluationRequest]()

  def loadScriptContent (resourcePath: String) = {
    val scriptStream = getClass.getResourceAsStream(resourcePath)
    scala.io.Source.fromInputStream(scriptStream).getLines.mkString("\n")
  }

  def generateLocalTopK(userVector: Vector, pruningStrategy: LEMPPruningStrategy): List[(ItemId, Double)] = {

    val topK = Types.createTopK
    val buckets = itemIdsDescendingByLength.toList.grouped(bucketSize)

    val userVectorLength = userVector.length


    breakable {
      for (currentBucket <- buckets) {
        if ( !((topK.length < workerK) || (currentBucket.head._2 * userVectorLength > topK.head._2))) {
          break()
        }
        val (focus, focusSet) =  generateFocusSet(userVector, pruningStrategy)

        val candidates = pruneCandidateSet(topK, currentBucket, pruningStrategy, focus, focusSet, userVector)

        //TODO check math
       for (item <- candidates) {
          val userItemDotProduct = Vector.dotProduct(userVector, item._2)

          if (topK.size < workerK) {
            topK += ((item._1, userItemDotProduct))
          }
          else {
            if (topK.head._2 < userItemDotProduct) {
              topK.dequeue
              topK += (( item._1, userItemDotProduct))
            }
          }
        }
      }
    }
    topK.toList
  }


  //TODO check performance of conversion between Array[Double] and Vector
  def calculateNegativeSamples(itemId: Option[ItemId], userVector: Vector): Vector = {
    val possibleNegativeItems =
      itemId match {
        case Some(id) => itemIds.filterNot(_ == id)
        case None     => itemIds
      }

    (0 until  math.min(negativeSampleRate, possibleNegativeItems.length))
      .foldLeft(Vector(numFactors))((vector, _) => {
        val negItemId = possibleNegativeItems(Random.nextInt(possibleNegativeItems.length))
        val negItemVector = model(negItemId)

        val (userDelta, itemDelta) = SGDUpdater.delta(0.0, userVector.value, negItemVector.value)
        model(negItemId) = Vector(Vector.vectorSum(itemDelta, negItemVector.value))
        Vector.vectorSum(Vector(userDelta), vector)
      })
  }

  //TODO Check logic
  def generateFocusSet(userVector: Vector, pruning: LEMPPruningStrategy): (Int, Array[Int]) = {
    val focus = ((1 until userVector.value.length) :\ 0) { (i, f) =>
      if (userVector.value(i) * userVector.value(i) > userVector.value(f) * userVector.value(f))
        i
      else
        f
    }

    // focus coordinate set for incremental pruning test
    val focusSet = Array.range(0, userVector.value.length - 1)
      .sortBy{ x => -userVector.value(x) * userVector.value(x) }
      .take(pruning match {
        case INCR(x) => x
        case LI(x, _)=> x
        case _=> 0
      })

    (focus, focusSet)
  }

  def pruneCandidateSet(topK: mutable.PriorityQueue[(ItemId, Double)], currentBucket: List[(ItemId, Double)],
                        pruning: LEMPPruningStrategy,
                        focus: Int, focusSet: Array[Int],
                        userVector: Vector): List[(ItemId, Vector)] = {
    val theta = if (topK.length < workerK) 0.0 else topK.head._2
    val theta_b_q = theta / (currentBucket.head._2 * userVector.length)
    val vectors = currentBucket.map(x => (x._1, model(x._1)))



    vectors.filter(
      pruning match {
        case LENGTH() => lengthPruning(theta / userVector.length)
        case COORD() => coordPruning(focus, userVector, theta_b_q)
        case INCR(_) => incrPruning(focusSet, userVector, theta)
        case LC(threshold) =>
          if (currentBucket.head._2 > currentBucket.last._2 * threshold)
            lengthPruning(theta / userVector.length)
          else
            coordPruning(focus, userVector, theta_b_q)
        case LI(_, threshold) =>
          if (currentBucket.head._2 > currentBucket.last._2 * threshold)
            lengthPruning(theta / userVector.length)
          else
            incrPruning(focusSet, userVector, theta)
      })
  }

  def train(userVector: Vector, request: EvaluationRequest, itemVector: Vector): Vector = {
    val negativeUserDelta = calculateNegativeSamples(Some(request.itemId), userVector)
    val (positiveUserDelta, positiveItemDelta) = SGDUpdater.delta(request.rating, userVector.value, itemVector.value)

    val updatedItemVector = Vector.vectorSum(itemVector, Vector(positiveItemDelta))
    model.update(request.itemId, updatedItemVector)
    itemIdsDescendingByLength.add((request.itemId, updatedItemVector.length))
    Vector.vectorSum(negativeUserDelta, Vector(positiveUserDelta))
  }

  override def onPullReceive(msg: Messages.Message[Int, Long, Vector],
                             out: Collector[Types.ParameterServerOutput]): Unit = {
    //logger.info("User vector received by worker from Redis channel.")

    val userVector = msg.message.get

    val topK = generateLocalTopK(userVector, pruningStrategy)

    val _request = requestBuffer.get(msg.destination)

    _request match {
      case None =>
        // when the observation which initiated the db query belongs to another worker - output a local topK for the observation with dummy additional data:
        out.collect(EvaluationOutput(-1, msg.destination, topK, -1))

      case Some(request) =>
        // when the observation which initiated the db query belongs to this worker - output the local topK with the request data (itemId & ts):
        val itemVector = model.getOrElseUpdate(request.itemId, Vector(factorInitDesc.open().nextFactor(request.itemId)))

        val userDelta: Vector = train(userVector, request, itemVector)

        // update user vector in db with userDelta
        pushClient.evalSHA(pushScriptId.get, List(msg.source), userDelta.value.toList)

        out.collect(EvaluationOutput(request.itemId, request.evaluationId, topK, request.ts))
    }
  }

  override def onInputReceive(data: EvaluationRequest,
                              out: Collector[Types.ParameterServerOutput]): Unit = {
    //logger.info("Input received by worker.")

    requestBuffer.update(data.evaluationId, data)

    // Query user vector from db & send to channel for broadcasting:
    pullClient.evalSHA(pullScriptId.get, List(data.userId),
      List(data.evaluationId, channelName, numFactors, rangeMin, rangeMax))

    // No output is to be generated here.
  }
}
