import sbt.Keys._
import sbt._

import scala.language.postfixOps

val appVersion = "0.4.0"
val pluginVersion = "1.0.0"
val scalaJvmVersion = "2.12.8"

val scalaTestVersion = "3.0.1"
val slf4jVersion = "1.7.25"
val sparkVersion = "2.4.1"

lazy val testDependencies = Seq(
  libraryDependencies ++= Seq(
    "log4j" % "log4j" % "1.2.17",
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test
  ))

/////////////////////////////////////////////////////////////////////////////////
//      Root Project - builds all artifacts
/////////////////////////////////////////////////////////////////////////////////

lazy val root = (project in file("./app")).
  aggregate(core, language, spark_embedded, spark_generator).
  dependsOn(core, language, spark_embedded, spark_generator).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "qwery",
    organization := "com.qwery",
    description := "Qwery Application",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true
  )

/////////////////////////////////////////////////////////////////////////////////
//      Core Project
/////////////////////////////////////////////////////////////////////////////////

lazy val core = (project in file("./app/core")).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "core",
    organization := "com.qwery",
    description := "A SQL-like query language for performing ETL",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(

    ))

/////////////////////////////////////////////////////////////////////////////////
//      Language/Parsing Project
/////////////////////////////////////////////////////////////////////////////////

lazy val language = (project in file("./app/language")).
  dependsOn(core).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "language",
    organization := "com.qwery",
    description := "A SQL-like query language for performing ETL",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(

    ))

/////////////////////////////////////////////////////////////////////////////////
//      Platform Projects: Spark
/////////////////////////////////////////////////////////////////////////////////

lazy val spark_embedded = (project in file("./app/platform/spark/embedded")).
  dependsOn(core, language).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "platform-spark-embedded",
    organization := "com.qwery",
    description := "A SQL-like query language for Spark",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-avro" % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-hive" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion
    ))

lazy val spark_generator = (project in file("./app/platform/spark/generator")).
  dependsOn(core, language).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "platform-spark-generator",
    organization := "com.qwery",
    description := "A SQL-like query language for generating Spark/Scala code",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(

    ))

lazy val sbt_qwery = (project in file("./app/platform/spark/sbt-plugin")).
  aggregate(spark_generator).
  dependsOn(spark_generator).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
      name := "sbt-qwery",
      organization := "com.qwery",
      description := "SBT plugin for generating Spark/Scala code from an SQL query",
      version := pluginVersion,
      scalaVersion := scalaJvmVersion,
      scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
      scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
      sbtPlugin := true,
      scriptedBufferLog := false,
      libraryDependencies ++= Seq(
          "org.scala-sbt" %% "scripted-plugin" % sbtVersion.value
      ))

/////////////////////////////////////////////////////////////////////////////////
//      Publishing
/////////////////////////////////////////////////////////////////////////////////

/*
lazy val publishingSettings = Seq(
  sonatypeProfileName := "org.xerial",
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  pomExtra :=
    <url>https://github.com/ldaniels528/qwery</url>
      <licenses>
        <license>
          <name>MIT License</name>
          <url>http://www.opensource.org/licenses/mit-license.php</url>
        </license>
      </licenses>
      <scm>
        <connection>scm:git:github.com/ldaniels528/qwery.git</connection>
        <developerConnection>scm:git:git@github.com:ldaniels528/qwery.git</developerConnection>
        <url>github.com/ldaniels528/qwery.git</url>
      </scm>
      <developers>
        <developer>
          <id>ldaniels528</id>
          <name>Lawrence Daniels</name>
          <email>lawrence.daniels@gmail.com</email>
          <organization>io.scalajs</organization>
          <organizationUrl>https://github.com/scalajs-io</organizationUrl>
          <roles>
            <role>Project-Administrator</role>
            <role>Developer</role>
          </roles>
          <timezone>+7</timezone>
        </developer>
      </developers>
)
*/

// loads the Scalajs-io root project at sbt startup
onLoad in Global := (Command.process("project root", _: State)) compose (onLoad in Global).value
