package parameter.server.utils.datastreamlogger.impl

import parameter.server.utils.datastreamlogger.{DbWriter, LogDataStruct}
// TODO add couchbase client import

class CouchBaseWriter extends DbWriter {
  // TODO initialize client

  // TODO implement writeToDb:
  override def writeToDb(d: LogDataStruct): Unit = ???
}
