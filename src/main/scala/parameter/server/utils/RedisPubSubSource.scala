package parameter.server.utils

import com.redis._
import grizzled.slf4j.Logging
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class RedisPubSubSource (host: String, port: Integer, channelName: String) extends RichSourceFunction[String] with Logging {

  var sourceContext: SourceContext[String] = _
  var client: RedisClient = _

  override def open(parameters: Configuration) = {
    // Remark: parameters is said to be deprecated in Flink API.
    client = new RedisClient(host, port)
  }

  @volatile var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
    client.disconnect
  }

  def msgConsumeCallback(msg: PubSubMessage) = {
    //logger.info("Message received.")
    msg match {
      case S(channel, _) => // subscribe - do nothing.
      case U(channel, _) => // unsubscribe - do nothing.

      case M(channel, msgContent) =>
        sourceContext.collect(msgContent)

      case E(throwable) =>
        logger.error("Error message received from db channel.", throwable)
    }
  }

  override def run(ctx: SourceContext[String]): Unit = {
    sourceContext = ctx
    client.subscribe(channelName)(msgConsumeCallback(_))
    while (isRunning) {
      //for {
      //  data <- client. get data ...
      //} yield ctx.collect(data)
    }
  }

}
