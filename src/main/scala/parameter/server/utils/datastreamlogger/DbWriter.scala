package parameter.server.utils.datastreamlogger

trait DbWriter extends Serializable {
  // TODO error handling, exceptions
  def writeToDb(d: LogDataStruct): Unit

  def open: Unit

  def close: Unit
}
