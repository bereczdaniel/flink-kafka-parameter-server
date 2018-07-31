package parameter.server

import java.util.Properties

import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.Message
import parameter.server.logic.server.{ServerLogic, ServerLogicWrapper}
import parameter.server.logic.worker.{WorkerLogic, WorkerLogicWrapper}
import parameter.server.utils.Types.{ParameterServerOutput, WorkerInput}

class ParameterServer(env: StreamExecutionEnvironment,
                      src: DataStream[WorkerInput],
                      workerLogic: WorkerLogic, serverLogic: ServerLogic,
                      serverToWorkerParse: String => Message, workerToServerParse: String => Message,
                      host: String, port: Int, serverToWorkerTopic: String, workerToServerTopic: String,
                      broadcastServerToWorkers: Boolean = false) {

  lazy val properties = new Properties()

  def init(): Unit = {
    properties.setProperty("bootstrap.servers", host + "/" + port)
    properties.setProperty("group.id", "parameterServer")
  }

  def serverToWorker(): DataStream[Message] =
    env
      .addSource(new FlinkKafkaConsumer011[String](serverToWorkerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map(serverToWorkerParse)

  def workerToServer(): DataStream[Message] =
    env
      .addSource(new FlinkKafkaConsumer011[String](workerToServerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map[Message](workerToServerParse)
      .keyBy(_.destination)

  def workerInput(inputStream: DataStream[WorkerInput], serverToWorkerStream: DataStream[Message]): ConnectedStreams[Message, WorkerInput] = {
    if (broadcastServerToWorkers)
      serverToWorkerStream.broadcast
        .connect(inputStream.keyBy(_.destination))
    else
      serverToWorkerStream
        .connect(inputStream)
        .keyBy(_.destination, _.destination)
  }

  def workerStream(workerInputStream: ConnectedStreams[Message, WorkerInput]): DataStream[Either[ParameterServerOutput, Message]] =
    workerInputStream
      .flatMap(new WorkerLogicWrapper(workerLogic))

  def serverStream(serverInputStream: DataStream[Message]): DataStream[Either[ParameterServerOutput, Message]] =
    serverInputStream
    .flatMap(new ServerLogicWrapper(serverLogic))

  def serverToWorkerStream(serverLogicStream: DataStream[Either[ParameterServerOutput, Message]]): DataStream[ParameterServerOutput] = {
    serverLogicStream
      .flatMap[String]((value: Either[ParameterServerOutput, Message], out: Collector[String]) => {
      value match {
        case Right(message) =>
          out.collect(message.toString)
        case Left(_) =>
      }
    })
      .addSink(new FlinkKafkaProducer011[String](host + port, serverToWorkerTopic, new SimpleStringSchema()))

    serverLogicStream
      .flatMap[ParameterServerOutput]((value: Either[ParameterServerOutput, Message], out: Collector[ParameterServerOutput]) => {
      value match {
        case Left(serverOutput) =>
          out.collect(serverOutput)
        case Right(_) =>
      }
    })
  }

  def workerToServerStream(workerLogicStream: DataStream[Either[ParameterServerOutput, Message]]): DataStream[ParameterServerOutput] = {
    workerLogicStream
      .flatMap[String]((value: Either[ParameterServerOutput, Message], out: Collector[String]) => {
      value match {
        case Right(message) =>
          val a = message.toString
          out.collect(a)
        case Left(_) =>
      }
    })
      .addSink(new FlinkKafkaProducer011[String](host + port, workerToServerTopic,  new SimpleStringSchema()))

    workerLogicStream
      .flatMap[ParameterServerOutput]((value: Either[ParameterServerOutput, Message], out: Collector[ParameterServerOutput]) => {
      value match {
        case Left(workerOutput) =>
          out.collect(workerOutput)
        case Right(_) =>
      }
    })
  }
}
