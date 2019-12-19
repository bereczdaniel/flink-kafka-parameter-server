package hu.sztaki.ilab.ps.kafka

import hu.sztaki.ilab.ps.common.types.{ParameterServerOutput, WorkerInput}
import hu.sztaki.ilab.ps.kafka.communication.Messages.Message
import hu.sztaki.ilab.ps.kafka.logic.worker.WorkerLogic
import hu.sztaki.ilab.ps.kafka.utils.Utils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import matrix.factorization.types.Parameter

class WorkerOnlyParameterServer[T <: WorkerInput,
                      P <: Parameter,
                      WK, SK](
                               env: StreamExecutionEnvironment,
                               inputStream: DataStream[T],
                               workerLogic: WorkerLogic[WK, SK, T, P],
                               serverToWorkerSource: SourceFunction[String],
                               serverToWorkerParse: String => Message[SK, WK, P],
                               workerToServerSink: SinkFunction[Message[WK, SK, P]]
                             )
  extends ParameterServerSkeleton[T] (env: StreamExecutionEnvironment, inputStream: DataStream[T]) {

  def buildJobGraph(): DataStream[ParameterServerOutput] = {

    val (workerOutput, workerToServerStream) =
      Utils.splitStream(
        workerOutputStream(
          workerInput(
            inputStream, serverToWorker())
        ))

    submitToServerFromWorker(workerToServerStream)

    workerOutput
  }

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

  def workerOutputStream(workerInputStream: ConnectedStreams[Message[SK, WK, P], T]): DataStream[Either[ParameterServerOutput, Message[WK, SK, P]]] =
    workerInputStream
      .flatMap(workerLogic)

  def submitToServerFromWorker(ds: DataStream[Message[WK, SK, P]]): Unit =
    ds
      //.map(_.toString)
      .addSink(workerToServerSink)

}
