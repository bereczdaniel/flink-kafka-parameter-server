package parameter.server.utils.datastreamlogger

import org.apache.flink.api.common.functions.MapFunction

/**
  * The Flink map function that must be called whenever a timestamp log is needed:
  * It passes through the input data stream elements unchanged
  * and puts a log for each one into the db with the current timestamp
  * @param dbWriter            The writer implementation that actually stores the data in the db
  * @param getIdFromMessage    A function to query the data element id in the stream (a.k.a. observationId) from the message
  * @param logDataConstFields  The constant data fields for this logging procedure
  * @tparam M
  */
class DataStreamLoggerMap[M](
                              dbWriter: DbWriter,
                              getIdFromMessage: M => Long,
                              logDataConstFields: LogDataConstFields
  )
  extends MapFunction[M, M] {

    override def map(msg: M) = {
      dbWriter.writeToDb(LogDataStruct.createFromMessage(msg, getIdFromMessage, DataStreamLoggerMap.getCurrentTimestamp(), logDataConstFields))
      msg
    }
}

object DataStreamLoggerMap {
  def getCurrentTimestamp(): Long = {
    System.nanoTime() / 1000000 // millisec? - thr precision may be modified
  }
}
