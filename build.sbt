name := "spark-demo"
organization := "com.iisigroup"
version := "0.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
)
