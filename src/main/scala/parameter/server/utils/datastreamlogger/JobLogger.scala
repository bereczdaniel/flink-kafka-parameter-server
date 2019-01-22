package parameter.server.utils.datastreamlogger


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import parameter.server.utils.Types.WorkerInput

object JobLogger {

  def doWithLogging[S : TypeInformation, T : TypeInformation](inputStream: DataStream[S],
                                            dbw: DbWriter, env: StreamExecutionEnvironment, testProcessCategory: String,
                                            runJob: (DataStream[S], StreamExecutionEnvironment) => DataStream[T],
                                            getIdFromInputMessage: S => Long,
                                            getIdFromOutputMessage: T => Long
                         ): DataStream[T] = {
    import scala.collection.JavaConverters._
    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(Map("testProcessId" -> DataStreamLoggerMap.getCurrentTimestamp.toString).asJava))
    runJob(inputStream.map(new DataStreamLoggerMap[S](dbw, getIdFromInputMessage, "input", testProcessCategory)), env)
      .forward.map(new DataStreamLoggerMap[T](dbw, getIdFromOutputMessage, "output", testProcessCategory))
  }

}
