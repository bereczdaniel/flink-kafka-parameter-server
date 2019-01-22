package parameter.server.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import parameter.server.communication.Messages.Message
import parameter.server.kafka.logic.server.ServerLogic
import parameter.server.kafka.logic.worker.WorkerLogic
import parameter.server.utils.Types.{Parameter, WorkerInput}


/**
  * Parameter server architecture factory on top of Apache Flink DataStream API with Apache Kafka for the iteration
  * @tparam T: Data type of the input data stream
  * @tparam P: Data type of the parameter in the ML model
  * @tparam WK: Data type of the key used in the workers
  * @tparam SK: Data type of the key used in the servers
  */
class KafkaPsFactory[T <: WorkerInput,
                      P <: Parameter,
                      WK, SK] {

  lazy val properties = new Properties()

  /**
    * Parameter server architecture factory on top of Apache Flink DataStream API with Apache Kafka for the iteration
    *
    * @param env                      : Apache Flink DataStream environment
    * @param inputStream              : Input stream
    * @param workerLogic              : Behaviour for the worker nodes in the PS
    * @param serverLogic              : Behaviour for the server nodes in the PS
    * @param serverToWorkerParse      : Parse function to create message from string
    * @param workerToServerParse      : Parse function to create message from string
    * @param host                     : Host name for Kafka
    * @param port                     : Port for Kafka
    * @param serverToWorkerTopic      : Topic name for the server --> worker communication
    * @param workerToServerTopic      : Topic name for the worker --> server communication
    * @param broadcastServerToWorkers : Communication tpye for the server --> worker communication
    */
  def createPs(
                env: StreamExecutionEnvironment,
                inputStream: DataStream[T],
                workerLogic: WorkerLogic[WK, SK, T, P], serverLogic: ServerLogic[WK, SK, P],
                serverToWorkerParse: String => Message[SK, WK, P], workerToServerParse: String => Message[WK, SK, P],
                host: String, port: Int, serverToWorkerTopic: String, workerToServerTopic: String,
                broadcastServerToWorkers: Boolean = false
              ) = {
    properties.setProperty("bootstrap.servers", host + port)
    properties.setProperty("group.id", "parameterServer")

    new ParameterServer[T, P, WK, SK](
      env, inputStream, workerLogic, serverLogic,
      serverToWorkerParse, workerToServerParse,
      serverToWorkerSink = new FlinkKafkaProducer011[String](host + port, serverToWorkerTopic, new SimpleStringSchema()),
      serverToWorkerSource = new FlinkKafkaConsumer011[String](serverToWorkerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest(),
      workerToServerSink = new FlinkKafkaProducer011[String](host + port, workerToServerTopic, new SimpleStringSchema()),
      workerToServerSource = new FlinkKafkaConsumer011[String](workerToServerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest(),
      broadcastServerToWorkers
    )
  }

}
