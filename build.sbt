lazy val kafkaVersion = "1.0.1"
lazy val sparkVersion = "2.3.0"

lazy val commonSettings = Seq(
  name := "cps-state-replication",
  version := "0.0.1",
  organization := "org.sba_research",
  scalaVersion := "2.11.8"
)

lazy val customScalacOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused-import"
)

lazy val customLibraryDependencies = Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.1.0",

  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "connect-json" % kafkaVersion,

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "com.twitter" %% "bijection-avro" % "0.8.1",
  "com.twitter" %% "chill-avro" % "0.7.2",

  "com.typesafe" % "config" % "1.3.3",
  "net.ceedubs" %% "ficus" % "1.1.1",

  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",

  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

lazy val customDependencyOverrides = Set(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.1",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.1"
)

lazy val commonExcludeDependencies = Seq(
  "org.slf4j" % "slf4j-log4j12"
)

lazy val customJavaOptions = Seq(
  "-Xmx1024m",
  "-XX:-MaxFDLimit"
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(scalacOptions ++= customScalacOptions)
  .settings(dependencyOverrides ++= customDependencyOverrides)
  .settings(libraryDependencies ++= customLibraryDependencies)
  .settings(excludeDependencies ++= commonExcludeDependencies)
  .settings(fork in run := true)
  .settings(connectInput in run := true)
  .settings(javaOptions in run ++= customJavaOptions)
  .settings(
    scalastyleFailOnError := true,
    compileScalastyle := scalastyle.in(Compile).toTask("").value,
    (compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value,
    testScalastyle := scalastyle.in(Test).toTask("").value,
    (test in Test) := ((test in Test) dependsOn testScalastyle).value)
