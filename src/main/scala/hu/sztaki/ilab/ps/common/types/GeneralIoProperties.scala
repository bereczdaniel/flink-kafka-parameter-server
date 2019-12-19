package hu.sztaki.ilab.ps.common.types

case class GeneralIoProperties(parallelism: Int, inputFile: Option[String], outputFile: Option[String], snapshotLength: Int, withDataStreamLogger: Boolean,
                               logBackend: String, K: Int, doEvalAndWrite: Boolean,
                               inputMedia: Option[String], inputKafkaHostPort: Option[String], inputKafkaTopic: Option[String])
