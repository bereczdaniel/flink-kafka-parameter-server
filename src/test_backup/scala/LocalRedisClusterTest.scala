import scredis._
import scala.util.{ Success, Failure }

object LocalRedisClusterTest {

  def main(args: Array[String]): Unit = {
    // Creates a Redis instance with default configuration.
    // See reference.conf for the complete list of configurable parameters.
    val redis = Redis("localhost",30001)
    val redisCl = RedisCluster(Server("localhost",30001))

    // Import internal ActorSystem's dispatcher (execution context) to register callbacks
    //import redis.dispatcher
    import redisCl.dispatcher

    // Executing a non-blocking command and registering callbacks on the returned Future
    redisCl.hGetAll("my-hash") onComplete {
      case Success(content) => println(content)
      case Failure(e) => e.printStackTrace()
    }


    // Executes a blocking command using the internal, lazily initialized BlockingClient
    redisCl.lPop(/*0, */"queue")

    // Subscribes to a Pub/Sub channel using the internal, lazily initialized SubscriberClient
    redis.subscriber.subscribe("My Channel") {
      case message@PubSubMessage.Message(channel, messageBytes) => println(
        message.readAs[String]()
      )
      case PubSubMessage.Subscribe(channel, subscribedChannelsCount) => println(
        s"Successfully subscribed to $channel"
      )
    }

    // Shutdown all initialized internal clients along with the ActorSystem
    redis.quit()
  }

}
