name := "JsonTransformerETL"

version := "1.0"

scalaVersion := "2.12.11"

val sparkVersion = "3.0.0"

lazy val root = (project in file(".")).
  settings(
    name := "JsonTransformerETL",
    version := "1.0",
    scalaVersion := "2.12.11",
    mainClass in Compile := Some("com.me.jsontransformer.entry.ETLMainApp")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.zalando" %% "spark-json-schema" % "0.6.3",
  "com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.0.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

