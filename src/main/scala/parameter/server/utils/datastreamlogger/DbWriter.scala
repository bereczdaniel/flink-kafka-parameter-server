package parameter.server.utils.datastreamlogger

abstract class DbWriter {
  // TODO error handling, exceptions
  def writeToDb(d: LogDataStruct): Unit
}
