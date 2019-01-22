package parameter.server.utils.datastreamlogger

import org.apache.flink.api.java.utils.ParameterTool
import parameter.server.utils.datastreamlogger.impl.{ConsoleWriter, CouchBaseWriter}

object DbWriterFactory {

  /** Get the logging backend db
    *
    * @return DbWriter
    */
  def createDbWriter(dbBackend: String, parameters: ParameterTool): DbWriter = dbBackend match {
    case "couchbase" => CouchBaseWriter.createFromParameters(parameters)
    case "console" => new ConsoleWriter
    case "postgresql" =>
      // TODO
      throw new UnsupportedOperationException
    case _ => throw new UnsupportedOperationException
  }





}
