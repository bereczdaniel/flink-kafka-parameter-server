package parameter.server

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import parameter.server.communication.Messages.Message
import parameter.server.logic.server.ServerLogic
import parameter.server.logic.worker.WorkerLogic
import parameter.server.utils.Types.WorkerInput
import matrix.factorization.types.Parameter


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
              ): ParameterServerSkeleton[T] = {
    val kafkaServer = host + ":" + port
    properties.setProperty("bootstrap.servers", kafkaServer)
    properties.setProperty("group.id", "parameterServer")

    new ParameterServer[T, P, WK, SK](
      env, inputStream, workerLogic, serverLogic,
      serverToWorkerSink = new FlinkKafkaProducer011[String](kafkaServer, serverToWorkerTopic, new SimpleStringSchema()),
      serverToWorkerSource = new FlinkKafkaConsumer011[String](serverToWorkerTopic, new SimpleStringSchema(), properties).setStartFromLatest(),
      serverToWorkerParse = serverToWorkerParse,
      workerToServerSink = new FlinkKafkaProducer011[String](kafkaServer, workerToServerTopic, new SimpleStringSchema()),
      workerToServerSource = new FlinkKafkaConsumer011[String](workerToServerTopic, new SimpleStringSchema(), properties).setStartFromLatest(),
      workerToServerParse = workerToServerParse,
      broadcastServerToWorkers
    )
  }

}
