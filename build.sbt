ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "US_Energy",
    libraryDependencies += "commons-io" % "commons-io" % "2.11.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0",
    libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1"
  )


