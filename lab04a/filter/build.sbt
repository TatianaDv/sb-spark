
lazy val commonSettings = Seq(
  name := "filter",
  version := "1.0",
  scalaVersion := "2.11.12",
  libraryDependencies += "org.apache.spark" %%  "spark-core" % "2.4.6",
  libraryDependencies += "org.apache.spark" %%  "spark-sql" % "2.4.6",
  libraryDependencies += "org.apache.spark" %%  "spark-mllib" % "2.4.6"//,
  //libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
  //libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.9",
  //libraryDependencies += "org.postgresql" % "postgresql" % "42.2.12"

)
lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "filter_2.11-1.0.jar"