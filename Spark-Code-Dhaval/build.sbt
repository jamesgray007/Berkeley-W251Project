lazy val common = Seq(
  version := "0.1.0",
  scalaVersion := "2.10.5",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided",
    "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1",
    "com.typesafe" % "config" % "1.3.0"
  ),
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
     {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
     }
  }
)

lazy val trump_popularity = (project in file(".")).
  settings(common: _*).
  settings(
    name := "trump_popularity",
    mainClass in (Compile, run) := Some("trump_popularity.Main"))
