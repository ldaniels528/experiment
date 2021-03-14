import sbt.Keys._
import sbt._

import scala.language.postfixOps

val scalaVersion_2_12 = "2.12.13"
val scalaVersion_2_13 = "2.13.4"

val appVersion = "0.4.0"
val pluginVersion = "1.0.0"
val scalaAppVersion = scalaVersion_2_12

val akkaVersion = "2.6.9"
val akkaHttpVersion = "10.2.1"
val awsKinesisClientVersion = "1.14.0"
val awsSDKVersion = "1.11.946"
val liftJsonVersion = "3.3.0"
val scalaTestVersion = "3.1.0"
val slf4jVersion = "1.7.30"
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

crossScalaVersions := Seq(scalaVersion_2_12)

/////////////////////////////////////////////////////////////////////////////////
//      Root Project - builds all artifacts
/////////////////////////////////////////////////////////////////////////////////

lazy val root = (project in file("./app")).
  aggregate(core, util, language, database_core, database_client, database_jdbc).
  dependsOn(core, util, language, database_core, database_client, database_jdbc).
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
        "org.xerial.snappy" % "snappy-java" % "1.1.8.4"
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

/////////////////////////////////////////////////////////////////////////////////
//      Database Projects
/////////////////////////////////////////////////////////////////////////////////

/**
 * @example sbt "project database_core" test
 */
lazy val database_core = (project in file("./app/database-core")).
  dependsOn(core, util, language).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "database-core",
    organization := "com.qwery",
    description := "Qwery Database Collections",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(
      "commons-io" % "commons-io" % "2.6",
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ))

/**
  * @example sbt "project database_client" test
  */
lazy val database_client = (project in file("./app/database-client")).
  dependsOn(database_core).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "database-client",
    organization := "com.qwery",
    description := "Qwery Database Client",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(

    ))

/**
 * @example sbt "project database_jdbc" test
 */
lazy val database_jdbc = (project in file("./app/database-jdbc")).
  dependsOn(database_client).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "qwery-jdbc",
    organization := "com.qwery",
    description := "Qwery JDBC Driver",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("org", "apache", xs @ _*) => MergeStrategy.first
      case _ => MergeStrategy.first
    },
    libraryDependencies ++= Seq(

    ))

/**
 * @example sbt "project database_kinesis" test
 */
lazy val database_kinesis = (project in file("./app/database-kinesis")).
  dependsOn(database_client).
  //settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "database-kinesis",
    organization := "com.qwery",
    description := "Qwery AWS Kinesis Integration",
    version := appVersion,
    scalaVersion := scalaAppVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    mainClass in assembly := Some("com.qwery.database.awstools.kinesis.KinesisSync"),
    autoCompilerPlugins := true,
    libraryDependencies ++= Seq(
      "com.amazonaws" % "amazon-kinesis-client" % awsKinesisClientVersion
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
