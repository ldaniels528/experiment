import sbt.Keys._
import sbt.Project.projectToRef
import sbt._

import scala.language.postfixOps

val appVersion = "0.4.0"
val scalaJvmVersion = "2.11.12"

val akkaVersion = "2.5.2"
val awsVersion = "1.11.394"
val flinkVersion = "1.6.1"
val kafkaVersion = "0.10.2.1"
val scalaTestVersion = "3.0.1"
val slf4jVersion = "1.7.25"
val sparkVersion = "2.3.2"

/////////////////////////////////////////////////////////////////////////////////
//      Scala (JVM)
/////////////////////////////////////////////////////////////////////////////////

lazy val testDependencies = Seq(
  libraryDependencies ++= Seq(
    "log4j" % "log4j" % "1.2.17",
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test
  ))

lazy val root = (project in file("./app")).
  aggregate(core, language, platform_common, platform_spark, platform_sparkcode).
  dependsOn(core, language, platform_common, platform_spark, platform_sparkcode).
  settings(publishingSettings: _*).
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

lazy val core = (project in file("./app/core")).
  settings(publishingSettings: _*).
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
    coverageEnabled := true,
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % awsVersion
    ))

lazy val language = (project in file("./app/language")).
  dependsOn(core).
  settings(publishingSettings: _*).
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
    coverageEnabled := true,
    libraryDependencies ++= Seq(

    ))

lazy val platform_common = (project in file("./app/platform/common")).
  dependsOn(core, language).
  settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "platform-common",
    organization := "com.qwery",
    description := "A SQL-like query language for Flink",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    libraryDependencies ++= Seq(
      //
      // Kafka
      //"org.apache.kafka" %% "kafka" % kafkaVersion,
      //"org.apache.kafka" % "kafka-clients" % kafkaVersion excludeAll ExclusionRule(organization = "net.jpountz.lz4", name = "lz4"),
      //
      "net.liftweb" %% "lift-json" % "3.0.1"
    ))

lazy val platform_flink = (project in file("./app/platform/flink")).
  dependsOn(platform_common).
  settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "platform-flink",
    organization := "com.qwery",
    description := "A SQL-like query language for Flink",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    libraryDependencies ++= Seq(
      // Flink
      "org.apache.flink" %% "flink-scala" % flinkVersion,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
      "org.apache.flink" %% "flink-clients" % flinkVersion,
      "org.apache.flink" %% "flink-table" % flinkVersion
    ))

lazy val platform_spark = (project in file("./app/platform/spark")).
  dependsOn(platform_common).
  settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "platform-spark",
    organization := "com.qwery",
    description := "A SQL-like query language for Spark",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    libraryDependencies ++= Seq(
      // Spark
      "com.databricks" %% "spark-avro" % "4.0.0",
      "com.databricks" %% "spark-csv" % "1.5.0",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-hive" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion
    ))

lazy val platform_sparkcode = (project in file("./app/platform/sparkcode")).
  dependsOn(platform_common, platform_spark).
  settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "platform-spark-codegen",
    organization := "com.qwery",
    description := "A SQL-like query language for generating Spark/Scala code",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    libraryDependencies ++= Seq(
      // Spark
      "com.databricks" %% "spark-avro" % "4.0.0",
      "com.databricks" %% "spark-csv" % "1.5.0",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-hive" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion
    ))

/////////////////////////////////////////////////////////////////////////////////
//      Publishing
/////////////////////////////////////////////////////////////////////////////////

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

// loads the Scalajs-io root project at sbt startup
onLoad in Global := (Command.process("project root", _: State)) compose (onLoad in Global).value
