package parameter.server.redis

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.scala._
import parameter.server.communication.Messages.Message
import parameter.server.redis.logic.worker.WorkerLogic
import parameter.server.utils.Types.{Parameter, ParameterServerOutput, WorkerInput}

class ParameterServer[T <: WorkerInput,
                      P <: Parameter,
                      WK, SK](
                               env: StreamExecutionEnvironment,
                               src: DataStream[T],
                               workerLogic: WorkerLogic[WK, SK, T, P],
                               serverPubSubSource: RichSourceFunction[String],
                               serverToWorkerParse: String => Message[SK, WK, P]
                             ) {

  def start(): DataStream[ParameterServerOutput] = {
    init()
    workerStream(
      workerInput(
        src, serverToWorker())
    )
  }

  def init(): Unit = { }

  def serverToWorker(): DataStream[Message[SK, WK, P]] =
    env
      .addSource(serverPubSubSource)
      .map(serverToWorkerParse)

  def workerInput(inputStream: DataStream[T], serverToWorkerStream: DataStream[Message[SK, WK, P]]): ConnectedStreams[Message[SK, WK, P], T] = {
//    if (broadcastServerToWorkers)
      serverToWorkerStream.broadcast
        .connect(inputStream.keyBy(_.destination.hashCode()))
//    else
//      serverToWorkerStream
//        .connect(inputStream)
//        .keyBy(_.destination.hashCode(), _.destination.hashCode())
  }

  def workerStream(workerInputStream: ConnectedStreams[Message[SK, WK, P], T]): DataStream[ParameterServerOutput] =
    workerInputStream
      .flatMap(workerLogic)


}
