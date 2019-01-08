package parameter.server.utils.datastreamlogger.impl

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.{Bucket, Cluster, CouchbaseCluster}
import parameter.server.utils.datastreamlogger.{DbWriter, LogDataStruct}
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


class CouchBaseWriter(username: String, passworld: String, bucketname: String, nodes: String*) extends DbWriter {
  //TODO: nodes:* ???
  val cluster: CouchbaseCluster = CouchbaseCluster.create
  cluster.authenticate(username, passworld)
  val bucket: Bucket = cluster.openBucket(bucketname)
  var inputCounter: Int = _
  var writeCounter: Int = _

  def close = {
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

  var counter: Int = _

//  def main(args: Array[String]): Unit = {
//      val cluster = CouchbaseCluster.create
//      cluster.authenticate("admin", "admin123")
//      val bucket = cluster.openBucket("fmtest")
//      val user = JsonObject.empty()
//      .put("run_type", "async")
//      .put("run_id", "1545318356")
//      .put("elem_id", 5)
//      .put("proc_phase", "input")
//      .put("ts", 3545318366L)
//      val doc = JsonDocument.create("8", user)
////      println(bucket.insert(doc))
////      println(bucket.async.insert(doc))
//      bucket.async.insert(doc)
//          .subscribe(new Action1[JsonDocument] {
//            override def call(t: JsonDocument): Unit = println(t.id())
//          })
//
//    Thread.sleep(10000)
//
//
//      cluster.disconnect
//  }

  def main(args: Array[String]): Unit = {
      val cluster = CouchbaseCluster.create
      cluster.authenticate("admin", "admin123")
      val bucket = cluster.openBucket("asynctest")
      val user = JsonObject.empty()
      .put("run_type", "async")
      .put("run_id", "1545318356")
      .put("elem_id", 5)
      .put("proc_phase", "input")
      .put("ts", 3545318366L)

     val c =(100 until 200).map(q => JsonDocument.create(q.toString, user))


    c.foreach(bucket.async.insert(_)
        .subscribe(new Action1[JsonDocument] {
            override def call(t: JsonDocument): Unit = counter += 1
          }))

    while(counter != c.size) {
      Thread.sleep(1)
    }
      cluster.disconnect
  }

}
