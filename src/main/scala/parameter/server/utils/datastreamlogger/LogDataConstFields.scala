package parameter.server.utils.datastreamlogger

/**
  * Part of the log data which is independent (constant) of the stream data elements being processed
  * @param processStage         start/end or another intermediate processing phase name
  * @param testProcessId        the id of the test process run (preferably the starting ts)
  * @param testProcessCategory  the type of the test process run (e.g. kafka/redis/...)
  */
sealed case class LogDataConstFields(
  processStage: String,
  testProcessId: Long,
  testProcessCategory: String) {

  override def toString: String =
    s"$processStage,$testProcessId,$testProcessCategory"
}
