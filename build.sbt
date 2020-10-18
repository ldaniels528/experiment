import sbt.Keys._
import sbt._

import scala.language.postfixOps

val scalaVersion_2_11 = "2.11.12"
val scalaVersion_2_12 = "2.12.12"
val scalaVersion_2_13 = "2.13.3"

val appVersion = "0.4.0"
val pluginVersion = "1.0.0"
val scalaAppVersion = scalaVersion_2_12

val akkaVersion = "2.6.9"
val akkaHttpVersion = "10.2.1"
val scalaTestVersion = "3.1.0"
val slf4jVersion = "1.7.25"
val sparkVersion_2_3_x = "2.3.4"
val sparkVersion_2_4_x = "2.4.6"
val sparkVersion_3_0_x = "3.0.0"

lazy val testDependencies = Seq(
  libraryDependencies ++= Seq(
    "log4j" % "log4j" % "1.2.17",
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test
  ))

crossScalaVersions := Seq(scalaVersion_2_11, scalaVersion_2_12)

/////////////////////////////////////////////////////////////////////////////////
//      Root Project - builds all artifacts
/////////////////////////////////////////////////////////////////////////////////

lazy val root = (project in file("./app")).
  aggregate(core, util, language, spark_generator, spark_tools_2_4_x, spark_tools_3_0_x).
  dependsOn(core, util, language, spark_generator, spark_tools_2_4_x, spark_tools_3_0_x).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "qwery-bundle",
    organization := "com.qwery",
    description := "Qwery Application Bundle",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true
  )

/////////////////////////////////////////////////////////////////////////////////
//      Core Projects
/////////////////////////////////////////////////////////////////////////////////

lazy val util = (project in file("./app/util")).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
      name := "util",
      organization := "com.qwery",
      description := "Qwery Helpers and Utilities",
      version := appVersion,
      scalaVersion := scalaAppVersion,
      scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
      scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
      autoCompilerPlugins := true,
      libraryDependencies ++= Seq(

      ))

lazy val core = (project in file("./app/core")).
  dependsOn(util).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "core",
    organization := "com.qwery",
    description := "Qwery SQL Models",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(

    ))

lazy val language = (project in file("./app/language")).
  dependsOn(core, util).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "language",
    organization := "com.qwery",
    description := "Qwery SQL Language Parsers",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(

    ))

/**
 * @example sbt "project persistence" test
 */
lazy val persistence = (project in file("./app/persistence")).
  dependsOn(core, util, language).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "persistent-collections",
    organization := "com.qwery",
    description := "Qwery Persistent Collections",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "commons-io" % "commons-io" % "2.6",
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "net.liftweb" %% "lift-json" % "3.3.0"
    ))

/////////////////////////////////////////////////////////////////////////////////
//      Platform Projects: Spark
/////////////////////////////////////////////////////////////////////////////////

lazy val spark_generator = (project in file("./app/platform/spark/generator")).
  dependsOn(core, util, language).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "spark-generator",
    organization := "com.qwery",
    description := "A SQL-like query language for generating Spark/Scala code",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(

    ))

/*
lazy val spark_tools_2_3_x = (project in file("./app/platform/spark/tools/2.3.x")).
  dependsOn(util).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "spark-tools-v2_3",
    organization := "com.qwery",
    description := "Qwery Runtime Tools for Spark 2.3.x",
    version := appVersion,
    scalaVersion := scalaVersion_2_11,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion_2_3_x,
      "org.apache.spark" %% "spark-sql" % sparkVersion_2_3_x
    ))*/

lazy val spark_tools_2_4_x = (project in file("./app/platform/spark/tools/2.4.x")).
  dependsOn(util).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "spark-tools-v2_4",
    organization := "com.qwery",
    description := "Qwery Runtime Tools for Spark 2.4.x",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion_2_4_x,
      "org.apache.spark" %% "spark-sql" % sparkVersion_2_4_x
    ))

lazy val spark_tools_3_0_x = (project in file("./app/platform/spark/tools/3.0.x")).
  dependsOn(util).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "spark-tools-v3_0",
    organization := "com.qwery",
    description := "Qwery Runtime Tools for Spark 3.0.x",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion_3_0_x,
      "org.apache.spark" %% "spark-sql" % sparkVersion_3_0_x
    ))

lazy val sbt_qwery = (project in file("./app/platform/spark/sbt-plugin")).
  aggregate(core, util, language, spark_generator).
  dependsOn(core, util, language, spark_generator).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "sbt-qwery",
    organization := "com.qwery",
    description := "SBT plugin for generating Spark/Scala code from an SQL query",
    version := pluginVersion,
    scalaVersion := scalaAppVersion,
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
