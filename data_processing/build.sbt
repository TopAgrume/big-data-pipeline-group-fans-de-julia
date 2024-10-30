name := "DataProcessing"

version := "0.1"

scalaVersion := "2.12.10"  // Use Scala 2.12.x, as Spark 3.x is compatible with it

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0"
)

resolvers ++= Seq(
  "Confluent" at "https://packages.confluent.io/maven/",
  "Apache Repo" at "https://repo1.maven.org/maven2/",
  "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots/"
)

//ThisBuild / dependencyOverrides += "org.scala-lang" % "scala-reflect" % scalaVersion.value

mainClass in Compile := Some("DataProcessing")

// Avant de build, installer java11 openjdk
// sudo pacman -S jdk11-openjdk
javaHome := Some(file("/usr/lib/jvm/java-11-openjdk"))

fork in run := true
classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat


