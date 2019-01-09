package parameter.server.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import parameter.server.communication.Messages.Message
import parameter.server.kafka.logic.server.ServerLogic
import parameter.server.kafka.logic.worker.WorkerLogic
import parameter.server.utils.Types.{Parameter, ParameterServerOutput, ParameterServerSkeleton, WorkerInput}
import parameter.server.utils.Utils


/**
  * Parameter server architecture on top of Apache Flink DataStream API with Apache Kafka for the iteration
  * @param env: Apache Flink DataStream environment
  * @param inputStream: Input stream
  * @param workerLogic: Behaviour for the worker nodes in the PS
  * @param serverLogic: Behaviour for the server nodes in the PS
  * @param serverToWorkerParse: Parse function to create message from string
  * @param workerToServerParse: Parse function to create message from string
  * @param host: Host name for Kafka
  * @param port: Port for Kafka
  * @param serverToWorkerTopic: Topic name for the server --> worker communication
  * @param workerToServerTopic: Topic name for the worker --> server communication
  * @param broadcastServerToWorkers: Communication tpye for the server --> worker communication
  * @tparam T: Data type of the input data stream
  * @tparam P: Data type of the parameter in the ML model
  * @tparam WK: Data type of the key used in the workers
  * @tparam SK: Data type of the key used in the servers
  */
class ParameterServer[T <: WorkerInput,
                      P <: Parameter,
                      WK, SK](
                               env: StreamExecutionEnvironment,
                               inputStream: DataStream[T],
                               workerLogic: WorkerLogic[WK, SK, T, P], serverLogic: ServerLogic[WK, SK, P],
                               serverToWorkerParse: String => Message[SK, WK, P], workerToServerParse: String => Message[WK, SK, P],
                               host: String, port: Int, serverToWorkerTopic: String, workerToServerTopic: String,
                               broadcastServerToWorkers: Boolean = false) extends ParameterServerSkeleton {

  def start(): DataStream[ParameterServerOutput] = {
    init()

    val (serverOutput, serverToWorkerStream) =
      Utils.splitStream(
        serverOutputStream(
          workerToServer()))

    val (workerOutput, workerToServerStream) =
      Utils.splitStream(
        workerOutputStream(
          workerInput(
            inputStream, serverToWorker())
      ))

    submitToKafkaTopic(serverToWorkerStream, serverToWorkerTopic)
    submitToKafkaTopic(workerToServerStream, workerToServerTopic)
    connectOutputStreams(serverOutput,workerOutput)
  }

  lazy val properties = new Properties()

  /**
    * Set the properties for Kafka
    */
  def init(): Unit = {
    properties.setProperty("bootstrap.servers", host + port)
    properties.setProperty("group.id", "parameterServer")
  }


  /**
    * Add the incoming messages as a Kafka source and parse them
    * @return Messages from server to the workers
    */
  def serverToWorker(): DataStream[Message[SK, WK, P]] =
    env
      .addSource(new FlinkKafkaConsumer011[String](serverToWorkerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map(serverToWorkerParse)

  /**
    * Add the incoming messages as a Kafka source, parse them and key them by their hashcode
    * @return Messages from worker to the server
    */
  def workerToServer(): DataStream[Message[WK, SK, P]] =
    env
      .addSource(new FlinkKafkaConsumer011[String](workerToServerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map[Message[WK, SK, P]](workerToServerParse)
      .keyBy(_.destination.hashCode())

  /**
    * Connects and partitions the incoming streams of the worker nodes (outer world + server messages)
    * @param inputStream: Datastream from the outer world
    * @param serverToWorkerStream: Messages from the server
    * @return Connected, partitioned stream
    */
  def workerInput(inputStream: DataStream[T], serverToWorkerStream: DataStream[Message[SK, WK, P]]): ConnectedStreams[Message[SK, WK, P], T] = {
    if (broadcastServerToWorkers)
      serverToWorkerStream.broadcast
        .connect(inputStream.keyBy(_.destination.hashCode()))
    else
      serverToWorkerStream
        .connect(inputStream)
        .keyBy(_.destination.hashCode(), _.destination.hashCode())
  }

  /**
    * Process the incoming stream (outer world + messages) based on the worker logic
    * @param workerInputStream: Input stream for the workers
    * @return Output (messages to the server + events to the outer world)
    */
  def workerOutputStream(workerInputStream: ConnectedStreams[Message[SK, WK, P], T]): DataStream[Either[ParameterServerOutput, Message[WK, SK, P]]] =
    workerInputStream
      .flatMap(workerLogic)

  /**
    * Process the incoming stream (messages from the worker) based on the server logic
    * @param serverInputStream: Input stream for the servers
    * @return Output (messages to the worker + event to the outer world)
    */
  def serverOutputStream(serverInputStream: DataStream[Message[WK, SK, P]]): DataStream[Either[ParameterServerOutput, Message[SK, WK, P]]] =
    serverInputStream
      .process(serverLogic)

  def submitToKafkaTopic[A](ds: DataStream[A], topic: String): Unit = 
    ds
    .map(_.toString)
    .addSink(new FlinkKafkaProducer011[String](host + port, topic, new SimpleStringSchema()))

  /**
    * Connect the events for the outer world from the servers and workers
    * @param serverOutput: Event for the outer world from the server
    * @param workerOutput: Event for the outer world from the worker
    * @return Events for the outer world from the server and the worker
    */
  def connectOutputStreams(serverOutput: DataStream[ParameterServerOutput],
                           workerOutput: DataStream[ParameterServerOutput]): DataStream[ParameterServerOutput] =
    serverOutput
    .connect(workerOutput)
    .map(so => so, wo => wo)

}
