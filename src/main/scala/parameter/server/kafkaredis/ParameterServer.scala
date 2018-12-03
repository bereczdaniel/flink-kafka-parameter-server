package parameter.server.kafkaredis

import java.util.Properties

import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.Message
import parameter.server.kafkaredis.logic.server.ServerLogic
import parameter.server.kafkaredis.logic.worker.WorkerLogic
import parameter.server.utils.Types.{Parameter, ParameterServerOutput, WorkerInput}

class ParameterServer[T <: WorkerInput,
                      P <: Parameter,
                      WK, SK](
                               env: StreamExecutionEnvironment,
                               src: DataStream[T],
                               workerLogic: WorkerLogic[WK, SK, T, P], serverLogic: ServerLogic[WK, SK, P],
                               serverToWorkerParse: String => Message[SK, WK, P], workerToServerParse: String => Message[WK, SK, P],
                               host: String, port: Int, serverToWorkerTopic: String, workerToServerTopic: String,
                               broadcastServerToWorkers: Boolean = false) {

  def start(): DataStream[ParameterServerOutput] = {
    init()

    workerToServerStream(
      workerStream(
        workerInput(
          src, serverToWorker())
      )).connect(
      serverToWorkerStream(
        serverStream(
          workerToServer())
      )).map(w => w, s => s)
  }

  lazy val properties = new Properties()

  def init(): Unit = {
    properties.setProperty("bootstrap.servers", host + port)
    properties.setProperty("group.id", "parameterServer")
  }

  def serverToWorker(): DataStream[Message[SK, WK, P]] =
    env
      .addSource(new FlinkKafkaConsumer011[String](serverToWorkerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map(serverToWorkerParse)

  def workerToServer(): DataStream[Message[WK, SK, P]] =
    env
      .addSource(new FlinkKafkaConsumer011[String](workerToServerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map[Message[WK, SK, P]](workerToServerParse)
      .keyBy(_.destination.hashCode())

  def workerInput(inputStream: DataStream[T], serverToWorkerStream: DataStream[Message[SK, WK, P]]): ConnectedStreams[Message[SK, WK, P], T] = {
    if (broadcastServerToWorkers)
      serverToWorkerStream.broadcast
        .connect(inputStream.keyBy(_.destination.hashCode()))
    else
      serverToWorkerStream
        .connect(inputStream)
        .keyBy(_.destination.hashCode(), _.destination.hashCode())
  }

  def workerStream(workerInputStream: ConnectedStreams[Message[SK, WK, P], T]): DataStream[Either[ParameterServerOutput, Message[WK, SK, P]]] =
    workerInputStream
      .flatMap(workerLogic)

  def serverStream(serverInputStream: DataStream[Message[WK, SK, P]]): DataStream[Either[ParameterServerOutput, Message[SK, WK, P]]] =
    serverInputStream
      .process(serverLogic)

  def serverToWorkerStream(serverLogicStream: DataStream[Either[ParameterServerOutput,  Message[SK, WK, P]]]): DataStream[ParameterServerOutput] = {
    serverLogicStream
      .flatMap[String]((value: Either[ParameterServerOutput,  Message[SK, WK, P]], out: Collector[String]) => {
      value match {
        case Right(message) =>
          out.collect(message.toString)
        case Left(_) =>
      }
    })
      .addSink(new FlinkKafkaProducer011[String](host + port, serverToWorkerTopic, new SimpleStringSchema()))

    serverLogicStream
      .flatMap[ParameterServerOutput]((value: Either[ParameterServerOutput, Message[SK, WK, P]], out: Collector[ParameterServerOutput]) => {
      value match {
        case Left(serverOutput) =>
          out.collect(serverOutput)
        case Right(_) =>
      }
    })
  }

  def workerToServerStream(workerLogicStream: DataStream[Either[ParameterServerOutput,  Message[WK, SK, P]]]): DataStream[ParameterServerOutput] = {
    workerLogicStream
      .flatMap[String]((value: Either[ParameterServerOutput, Message[WK, SK, P]], out: Collector[String]) => {
      value match {
        case Right(message) =>
          val a = message.toString
          out.collect(a)
        case Left(_) =>
      }
    })
      .addSink(new FlinkKafkaProducer011[String](host + port, workerToServerTopic,  new SimpleStringSchema()))

    workerLogicStream
      .flatMap[ParameterServerOutput]((value: Either[ParameterServerOutput, Message[WK, SK, P]], out: Collector[ParameterServerOutput]) => {
      value match {
        case Left(workerOutput) =>
          out.collect(workerOutput)
        case Right(_) =>
      }
    })
  }
}
