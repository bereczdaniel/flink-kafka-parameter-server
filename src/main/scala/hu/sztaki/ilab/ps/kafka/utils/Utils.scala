package hu.sztaki.ilab.ps.kafka.utils

import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool

object Utils {

  /**
    * Loads a script file as a single string from a resource path
    */
  def loadTextFileContent(resourcePath: String): String = {
    val textFileStream = getClass.getResourceAsStream(resourcePath)
    scala.io.Source.fromInputStream(textFileStream).getLines.mkString("\n")
  }

  /**
    * Split a DataStream of Either to two separate stream
    * @param ds
    * @tparam A
    * @tparam B
    * @return DataStream of the left values, DataStream of the right values
    */
  def splitStream[A,B]
  (ds: DataStream[Either[A,B]])  (implicit 
  tiA: TypeInformation[A],
  tiB: TypeInformation[B]) : (DataStream[A], DataStream[B]) = 
    (ds.flatMap[A]((value: Either[A,B], out: Collector[A]) => {
      value match {
        case Left(aa) =>
          out.collect(aa)
        case Right(_) =>
      }
    }),
    ds.flatMap[B]((value: Either[A,B], out: Collector[B]) => {
      value match {
        case Right(bb) =>
          out.collect(bb)
        case Left(_) =>
      }
    }))

}
