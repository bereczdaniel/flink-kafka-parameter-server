package parameter.server.utils.datastreamlogger

trait DbWriter {
  // TODO error handling, exceptions
  def writeToDb(d: LogDataStruct): Unit

  def close: Unit
}
