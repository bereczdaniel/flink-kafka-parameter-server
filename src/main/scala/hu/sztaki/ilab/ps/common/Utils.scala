package hu.sztaki.ilab.ps.common

import org.apache.flink.api.java.utils.ParameterTool

object Utils {
  /**
    * Get the params for the properties file specified as the first parameter
    *
    * @param args
    * @return
    */
  def getProperties(args: Array[String]): Option[ParameterTool] = {
    def parameterCheck(args: Array[String]): Option[String] = {
      def outputNoParamMessage(): Unit = {
        val noParamMsg = "\tUsage:\n\n\t./run <path to parameters file>"
        println(noParamMsg)
      }

      if (args.length == 0 || !(new java.io.File(args(0)).exists)) {
        outputNoParamMessage()
        None
      } else {
        Some(args(0))
      }
    }

    parameterCheck(args).map(propsPath => ParameterTool.fromPropertiesFile(propsPath))
  }
}

