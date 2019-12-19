package hu.sztaki.ilab.ps.main

import hu.sztaki.ilab.ps.common.PsWrapper
import hu.sztaki.ilab.ps.common.types.{GeneralIoProperties, GeneralMfProperties, ParameterServerOutput, RecSysMessages}
import hu.sztaki.ilab.ps.kafka.algorithms.matrix.factorization.MfPsFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

class FlinkKafkaPsWrapper extends PsWrapper {

  override def buildPs(input: DataStream[RecSysMessages.EvaluationRequest], mfProperties: GeneralMfProperties,
                       ioProperties: GeneralIoProperties, parameterTool: ParameterTool, env: StreamExecutionEnvironment)
  : DataStream[_ <:ParameterServerOutput] =
    MfPsFactory.createPs(parameterTool.get("psImplType", "kafka"), mfProperties, parameterTool, input, env).buildJobGraph()

}

