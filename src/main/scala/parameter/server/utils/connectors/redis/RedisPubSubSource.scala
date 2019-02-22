package parameter.server.utils.connectors.redis

import scredis._
import grizzled.slf4j.Logging
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class RedisPubSubSource (host: String, port: Integer, channelName: String) extends RichSourceFunction[String] with Logging {

  var sourceContext: SourceContext[String] = _
//  var client: Redis = _
  lazy val client: Redis = new Redis(host, port)

  override def open(parameters: Configuration) = {
    // Remark: parameters is said to be deprecated in Flink API.
//    client = new Redis(host, port)
  }

  @volatile var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
    client.quit()
  }

  def msgConsumeCallback: PartialFunction[PubSubMessage, Unit] = {
    //logger.info("Message received.")
    //import PubSubMessage._
    //msg match {
    //  case Subscribe(channel, _) => // subscribe - do nothing.
    //  case Unsubscribe(channel, _) => // unsubscribe - do nothing.

      case message@PubSubMessage.Message(_, _) =>
        sourceContext.collect(message.readAs[String])

      //case E(throwable) =>
      //  logger.error("Error message received from db channel.", throwable)
    //}
  }

  override def run(ctx: SourceContext[String]): Unit = {
    sourceContext = ctx
    client.subscriber.subscribe(channelName)(msgConsumeCallback)
    while (isRunning) {
      //for {
      //  data <- client. get data ...
      //} yield ctx.collect(data)
    }
  }

}
