package parameter.server.utils.datastreamlogger.impl

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.kafka.clients.producer._
import parameter.server.utils.datastreamlogger.{DbWriter, LogDataStruct}

class KafkaWriter(topic: String, host: String, port: String) extends DbWriter{

  val  props = new Properties()

  lazy val producer = new KafkaProducer[String, String](props)


  override def writeToDb(d: LogDataStruct): Unit = {
    producer.send(new ProducerRecord(topic, "key", d.toString))
  }

  override def open: Unit = {
    props.put("bootstrap.servers", s"$host:$port")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  }

  override def close: Unit = {
    producer.close()
  }
}

object KafkaWriter {
  def createFromParameters(parameters: ParameterTool): KafkaWriter = {
    val topic = parameters.get("kafka.topic")
    val host = parameters.get("kafka.host")
    val port = parameters.get("kafka.port")
    new KafkaWriter(topic, host, port)
  }
}
