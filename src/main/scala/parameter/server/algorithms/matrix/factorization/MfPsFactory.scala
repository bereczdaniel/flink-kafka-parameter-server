package parameter.server.algorithms.matrix.factorization
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import parameter.server.algorithms.factors.RangedRandomFactorInitializerDescriptor
import parameter.server.algorithms.matrix.factorization.RecSysMessages.EvaluationRequest
import parameter.server.algorithms.matrix.factorization.kafka.KafkaMfPsFactory
import parameter.server.algorithms.matrix.factorization.kafkaredis.KafkaredisMfPsFactory
import parameter.server.algorithms.matrix.factorization.redis.RedisMfPsFactory
import parameter.server.utils.Types.ParameterServerSkeleton


trait MfPsFactory {

  def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton

}

object MfPsFactory {

  /** Get the logging backend db
    *
    * @return DbWriter
    */
  def createPs(psImplType: String,
               parameters: ParameterTool,
               inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton = {
    val generalParam = parseGeneralParameters(parameters)
    (psImplType match {
      case "kafka" => new KafkaMfPsFactory
      case "kafkaredis" => new KafkaredisMfPsFactory
      case "redis" => new RedisMfPsFactory
      case _ => throw new UnsupportedOperationException
    }).createPs(generalParam,
      parameters, RangedRandomFactorInitializerDescriptor(generalParam.numFactors, generalParam.rangeMin, generalParam.rangeMax),
      inputStream, env)
  }

  def parseGeneralParameters(parameters: ParameterTool): GeneralMfProperties = {
    val learningRate = parameters.getDouble("learningRate")
    val negativeSampleRate = parameters.getInt("negativeSampleRate")
    val numFactors = parameters.getInt("numFactors")
    val rangeMin = parameters.getDouble("rangeMin")
    val rangeMax = parameters.getDouble("rangeMax")
    val workerK = parameters.getInt("workerK")
    val bucketSize = parameters.getInt("bucketSize")

    GeneralMfProperties(learningRate, numFactors, negativeSampleRate, rangeMin, rangeMax,
                     workerK, bucketSize)
  }

}
