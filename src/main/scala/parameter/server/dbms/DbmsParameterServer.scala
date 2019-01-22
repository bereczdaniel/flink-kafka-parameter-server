package parameter.server.dbms

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import parameter.server.ParameterServerSkeleton
import parameter.server.communication.Messages.Message
import parameter.server.dbms.logic.worker.WorkerLogic
import parameter.server.utils.Types.{Parameter, ParameterServerOutput, WorkerInput}

class DbmsParameterServer[T <: WorkerInput,
                      P <: Parameter,
                      WK, SK](
                               env: StreamExecutionEnvironment,
                               inputStream: DataStream[T],
                               workerLogic: WorkerLogic[WK, SK, T, P],
                               serverToWorkerSource: SourceFunction[String],
                               serverToWorkerParse: String => Message[SK, WK, P]
                             ) extends ParameterServerSkeleton {

  def start(): DataStream[ParameterServerOutput] = {
    init()

    WorkerOutputStream(
      workerInput(
        inputStream, serverToWorker())
    )
  }

  def init(): Unit = { }

  def serverToWorker(): DataStream[Message[SK, WK, P]] =
    env
      .addSource(serverToWorkerSource)
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

  def WorkerOutputStream(workerInputStream: ConnectedStreams[Message[SK, WK, P], T]): DataStream[ParameterServerOutput] =
    workerInputStream
      .flatMap(workerLogic)


}
