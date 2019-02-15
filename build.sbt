name := "flink-kafka-parameter-server"

version := "0.1"

scalaVersion := "2.11.7"

lazy val flinkVersion = "1.7.1"
lazy val breezeVersion = "0.13"
lazy val loggerVersion = "1.7.25"

lazy val commonDependencies = Seq(
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "org.slf4j" % "slf4j-api" % loggerVersion,
  "com.typesafe" % "config" % "1.3.1"
)

lazy val json4s = Seq(
  "org.json4s" %% "json4s-native" % "3.6.0-M1",
  "org.json4s" %% "json4s-jackson" % "3.6.0-M1"
)

lazy val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion,
  //"org.apache.flink" % "flink-connector-kafka-0.11_2.11" % flinkVersion,  - not provided in flink cluster, see at compile dependencies
  "org.apache.flink" %% "flink-test-utils" % flinkVersion
)

lazy val couchbaseDependencies = Seq(
  "com.couchbase.client" % "java-client" % "2.7.2"
)


lazy val breezeDependencies = Seq(
  "org.scalanlp" %% "breeze" % breezeVersion,
  // Native libraries are not included by default. add this if you want them (as of 0.7)
  // Native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
  "org.scalanlp" %% "breeze-natives" % breezeVersion
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies,
    //libraryDependencies ++= flinkDependencies.map(_ % "provided"), // for flink submission
    libraryDependencies ++= flinkDependencies.map(_ % "compile"),    // for standalone running
    libraryDependencies ++= breezeDependencies.map(_ % "compile"),
    libraryDependencies ++= couchbaseDependencies,
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-connector-kafka-0.11_2.11" % flinkVersion,
      "com.github.scredis" %% "scredis" % "2.2.3",    // https://github.com/scredis/scredis
      "com.typesafe.akka" %% "akka-actor" % "2.5.17",
      "com.typesafe.akka" %% "akka-protobuf" % "2.5.17",
      "com.typesafe.akka" %% "akka-slf4j" % "2.5.17",
      "com.typesafe.akka" %% "akka-stream" % "2.5.17",
      "org.slf4j" % "slf4j-simple" % loggerVersion,
      "org.clapper" %% "grizzled-slf4j" % "1.0.2"
    )
  )

lazy val commonSettings = Seq(
  organization := "hu.sztaki.ilab",
  version := "0.1.0",
  scalaVersion := "2.11.7",
  test in assembly := {}
)

