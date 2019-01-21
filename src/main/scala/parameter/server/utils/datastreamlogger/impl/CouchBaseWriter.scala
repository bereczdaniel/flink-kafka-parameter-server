package parameter.server.utils.datastreamlogger.impl

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.{Bucket, CouchbaseCluster}
import org.apache.flink.api.java.utils.ParameterTool
import parameter.server.utils.datastreamlogger.{DataStreamLoggerMap, DbWriter, LogDataConstFields, LogDataStruct}
import rx.functions.Action1

//class CouchBaseWriter extends DbWriter {
//  //TODO: nodes:* ???
//  var cluster: CouchbaseCluster = _
//  var bucket: Bucket = _
//  var inputCounter: Int = _
//  var writeCounter: Int = _
//
//
//  def init(username: String, passworld: String, bucketname: String, nodes: String*): Unit = {
//    cluster = CouchbaseCluster.create
//    cluster.authenticate(username, passworld)
//    bucket = cluster.openBucket(bucketname)
//  }
//
//  def close = {
//    while(inputCounter > writeCounter) {
//      Thread.sleep(1)
//    }
//      cluster.disconnect
//  }
//
//  private def convertLogDataStructToJson(d: LogDataStruct) =
//    JsonDocument.create(d.hashCode.toString, JsonObject.empty()
//      .put("elem_id", d.observationId)
//      .put("ts", d.timestamp)
//      .put("run_type", d.constFields.testProcessCategory)
//      .put("run_id", d.constFields.testProcessId)
//      .put("proc_phase", d.constFields.processStage))
//
//  override def writeToDb(d: LogDataStruct): Unit = {
//    bucket.async.insert(convertLogDataStructToJson(d))
//        .subscribe(new Action1[JsonDocument] {
//            override def call(t: JsonDocument): Unit = writeCounter += 1
//          })
//    inputCounter += 1
//  }
//}


class CouchBaseWriter(username: String, password: String, bucketname: String, nodes: String*) extends DbWriter {
  val cluster: CouchbaseCluster = CouchbaseCluster.create(nodes: _*)
  cluster.authenticate(username, password)
  val bucket: Bucket = cluster.openBucket(bucketname)
  var inputCounter: Int = _
  var writeCounter: Int = _

   override def close = {
    while(inputCounter > writeCounter) {
      Thread.sleep(1)
    }
      cluster.disconnect
  }

  private def convertLogDataStructToJson(d: LogDataStruct) =
    JsonDocument.create(d.hashCode.toString, JsonObject.empty()
      .put("elem_id", d.observationId)
      .put("ts", d.timestamp)
      .put("run_type", d.constFields.testProcessCategory)
      .put("run_id", d.constFields.testProcessId)
      .put("proc_phase", d.constFields.processStage))

  override def writeToDb(d: LogDataStruct): Unit = {
    bucket.async.insert(convertLogDataStructToJson(d))
        .subscribe(new Action1[JsonDocument] {
            override def call(t: JsonDocument): Unit = writeCounter += 1
          })
    inputCounter += 1
  }
}

object CouchBaseWriter {

  def getFromParameters(parameters: ParameterTool) = {
    val username = parameters.get("couchbase_username", "admin")
    val passworld = parameters.get("couchbase_passworld", "admin123")
    val bucketname = parameters.get("couchbase_bucketname", "asynctest")
    val nodes = parameters.get("couchbase_nodes", "localhost").split(",")
    new CouchBaseWriter(username, passworld, bucketname, nodes:_*)
  }

  def main(args: Array[String]): Unit = {
    val cw = new CouchBaseWriter("admin", "admin123", "asynctest", "localhost")
    (0 until 100).map(
      LogDataStruct.createFromMessage[Long](_, x=>x, DataStreamLoggerMap.getCurrentTimestamp,
        new LogDataConstFields("input", 456, "kafka")))
      .foreach(cw.writeToDb)

    cw.close
  }

}
