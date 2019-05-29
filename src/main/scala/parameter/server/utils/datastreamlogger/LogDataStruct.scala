package parameter.server.utils.datastreamlogger

/**
  * The basic data structure to be written to db as a log event tuple
  * @param observationId     id of the stream data element (observation)
  * @param timestamp         the timestamp when the message is passed to this phase (Long)
  * @param constFields       the constant data fields for this logging procedure
  */
sealed case class LogDataStruct(
    observationId: Long,
    timestamp: Long,
    constFields: LogDataConstFields) {

  override def toString: String =
    s"$observationId,$timestamp,$constFields"
}

object LogDataStruct {

  /**
    * Create a data element directly from a message passed through the stream
    * @param msg                  The message passed through the data stream, whose processing is being logged
    * @param getIdFromMessage     Gets the observation id from the message, depending on its current format in the processing pipeline
    * @param timestamp
    * @param constFields       the constant properties data for this logging procedure
    * @tparam T
    * @return
    */
  def createFromMessage[T](
      msg: T,
      getIdFromMessage: T => Long,
      timestamp: Long,
      constFields: LogDataConstFields) = {
    new LogDataStruct(getIdFromMessage(msg), timestamp, constFields)
  }
}
