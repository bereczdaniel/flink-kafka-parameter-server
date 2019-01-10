package parameter.server.utils.datastreamlogger


import org.apache.flink.api.java.utils.ParameterTool
import parameter.server.algorithms.matrix.factorization.RecSysMessages.{EvaluationOutput, EvaluationRequest}
import parameter.server.communication.Messages.NotSupportedOutput
import parameter.server.utils.Types.ParameterServerSkeleton
import org.apache.flink.streaming.api.scala._


object LogFrame {

  def addLogFrameAndRunPS(inputStream: DataStream[EvaluationRequest], inputToPS: DataStream[EvaluationRequest] => ParameterServerSkeleton,
                  dbw: DbWriter, env: StreamExecutionEnvironment, testProcessCategory: String): DataStream[EvaluationOutput] = {
    import scala.collection.JavaConverters._
    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(Map("testProcessId" -> DataStreamLoggerMap.getCurrentTimestamp.toString).asJava))
    // TODO: evaluationId is ok for observationId?
    inputToPS(inputStream.map(new DataStreamLoggerMap[EvaluationRequest](dbw, _.evaluationId, "input", testProcessCategory)))
    .start()
    .flatMap(_ match {
          case eval: EvaluationOutput => Some(eval)
          case _ => throw new NotSupportedOutput
        })
    .map(new DataStreamLoggerMap[EvaluationOutput](dbw, _.evaluationId, "output", testProcessCategory))
  }

}
