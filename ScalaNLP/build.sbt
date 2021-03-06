name := "ScalaNLP"

version := "0.1"
scalaVersion := "2.11.8"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.5.2",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.3.0",
  "org.scalactic" %% "scalactic" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "com.lihaoyi" %% "upickle" % "0.7.1")