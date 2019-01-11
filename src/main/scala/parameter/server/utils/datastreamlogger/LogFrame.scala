package parameter.server.utils.datastreamlogger


import org.apache.flink.api.java.utils.ParameterTool
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.utils.Types.Recommendation
import org.apache.flink.streaming.api.scala._


object LogFrame {

  def addLogFrameAndRunPS(inputStream: DataStream[EvaluationRequest],
                          dbw: DbWriter, env: StreamExecutionEnvironment, testProcessCategory: String,
                          runPS: (DataStream[EvaluationRequest], StreamExecutionEnvironment) => DataStream[Recommendation]
                         ): DataStream[Recommendation] = {
    import scala.collection.JavaConverters._
    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(Map("testProcessId" -> DataStreamLoggerMap.getCurrentTimestamp.toString).asJava))
    // TODO: evaluationId is ok for observationId?
    runPS(inputStream.map(new DataStreamLoggerMap[EvaluationRequest](dbw, _.evaluationId, "input", testProcessCategory)), env)
      .map(new DataStreamLoggerMap[Recommendation](dbw, _.evaluationId, "output", testProcessCategory))
  }

}
