package parameter.server.utils.datastreamlogger

trait DbWriter extends Serializable {
  // TODO error handling, exceptions
  def writeToDb(d: LogDataStruct): Unit

  def close: Unit
}
