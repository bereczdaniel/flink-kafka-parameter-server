package hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization

import hu.sztaki.ilab.ps.common.types.GeneralMfProperties
import hu.sztaki.ilab.ps.common.types.RecSysMessages.EvaluationRequest
import hu.sztaki.ilab.ps.kafka.ParameterServerSkeleton
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.impl.{KafkaMfPsFactory, KafkaRedisMfPsFactory, RedisMfPsFactory}
import matrix.factorization.initializer.RangedRandomFactorInitializerDescriptor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


trait MfPsFactory {

  def createPs(generalMfProperties: GeneralMfProperties,
                        parameters: ParameterTool, factorInitDesc: RangedRandomFactorInitializerDescriptor,
                        inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton[EvaluationRequest]

}

// TODO move the above abstract factory to common and use it for native flink as well (rename to buildPS and merge with runPS, and return a DataStream instead of Param.ServerSkeleton
// TODO rename the object below, it becomes a specific factory method for kafka&redis package, and add .start() to it, it becomes a buildPS for Kafka/Redis ps types

object MfPsFactory {

  /** Get the logging backend db
    *
    * @return DbWriter
    */
  def createPs(psImplType: String,
               mfProperties: GeneralMfProperties,
               parameters: ParameterTool,
               inputStream: DataStream[EvaluationRequest], env: StreamExecutionEnvironment): ParameterServerSkeleton[EvaluationRequest] = {
//    val generalParam = parseGeneralParameters(parameters)
    val q = (psImplType match {
      case "kafka" => new KafkaMfPsFactory
      case "kafkaredis" => new KafkaRedisMfPsFactory
      case "redis" => new RedisMfPsFactory
      case _ => throw new UnsupportedOperationException
    })
//      .createPs(mfProperties,
//      parameters, RangedRandomFactorInitializerDescriptor(mfProperties.numFactors, mfProperties.randomInitRangeMin, mfProperties.randomInitRangeMax),
//      inputStream, env)
  }

//  def parseGeneralParameters(parameters: ParameterTool): GeneralMfProperties = {
//    val learningRate = parameters.getDouble("learningRate")
//    val negativeSampleRate = parameters.getInt("negativeSampleRate")
//    val numFactors = parameters.getInt("numFactors")
//    val rangeMin = parameters.getDouble("randomInitRangeMin")
//    val rangeMax = parameters.getDouble("randomInitRangeMax")
//    val workerK = parameters.getInt("workerK")
//    val bucketSize = parameters.getInt("bucketSize")
//
//    GeneralMfProperties(learningRate, numFactors, negativeSampleRate, rangeMin, rangeMax,
//                     workerK, bucketSize)
//  }

}
