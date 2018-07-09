lazy val flumeVersion = "1.8.0"

lazy val commonSettings = Seq(
  name := "cps-flume-interceptors",
  version := "0.0.1",
  organization := "org.sba_research",
  //scalaVersion := "2.11.8"
  /* Some Flume modules are not compatible with Scala 2.11. */
  scalaVersion := "2.10.6"
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
  "-Ywarn-numeric-widen" //,
  //"-Ywarn-unused-import"
)

lazy val customLibraryDependencies = Seq(
  "org.apache.flume" % "flume-ng-core" % flumeVersion,
  "org.apache.flume" % "flume-ng-sdk" % flumeVersion
)

lazy val customJavaOptions = Seq(
  "-Xmx1024m",
  "-XX:-MaxFDLimit"
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(scalacOptions ++= customScalacOptions)
  .settings(libraryDependencies ++= customLibraryDependencies)
  .settings(fork in run := true)
  .settings(connectInput in run := true)
  .settings(javaOptions in run ++= customJavaOptions)
  .settings(
    (compile in Compile) := (compile in Compile).value,
    (test in Test) := (test in Test).value
  )
