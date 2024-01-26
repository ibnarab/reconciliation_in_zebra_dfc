name := "reconciliation_in_zebra"

version := "0.1"

scalaVersion := "2.12.7"

val sparkVersion = "3.5.0"

/*libraryDependencies++= Seq(
  "org.scala-lang"    % "scala-compiler" % scalaVersion.value,
  "org.apache.spark" %% "spark-core"     % sparkVersion,
  "org.apache.spark" %% "spark-sql"      % sparkVersion,
  "org.apache.spark" %% "spark-hive"     % sparkVersion,
  "com.typesafe"      % "config"         % "1.2.1"
)*/

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.commons" % "commons-email" % "1.5",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.typesafe" % "config" % "1.2.1",
  "net.liftweb" %% "lift-json" % "3.1.1",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "8.1.0"
)