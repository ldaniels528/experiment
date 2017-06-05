import sbt.Keys.{libraryDependencies, _}
import sbt._

import scala.language.postfixOps

val apiVersion = "0.3.4"
val appScalaVersion = "2.12.2"

val akkaVersion = "2.5.2"
val curatorVersion = "3.1.0"
val kafkaVersion = "0.10.2.1"
val slf4jVersion = "1.7.25"

homepage := Some(url("https://github.com/ldaniels528/qwery"))

lazy val cli = (project in file("./app/cli")).
  aggregate(core).
  dependsOn(core).
  settings(
    name := "qwery-cli",
    organization := "com.github.ldaniels528",
    description := "Qwery CLI Application",
    version := apiVersion,
    scalaVersion := appScalaVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    mainClass in assembly := Some("com.github.ldaniels528.qwery.cli.QweryCLI"),
    test in assembly := {},
    assemblyJarName in assembly := s"${name.value}-${version.value}.bin.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("log4j.properties", _*) => MergeStrategy.discard
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17",
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.scala-lang" % "jline" % "2.11.0-M3",
      "org.slf4j" % "slf4j-api" % slf4jVersion
    ))

lazy val etl = (project in file("./app/etl")).
  aggregate(core).
  dependsOn(core).
  settings(
    name := "qwery-etl",
    organization := "com.github.ldaniels528",
    description := "Qwery ETL and Orchestration Server",
    version := apiVersion,
    scalaVersion := appScalaVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    mainClass in assembly := Some("com.github.ldaniels528.qwery.etl.QweryETL"),
    test in assembly := {},
    assemblyJarName in assembly := s"${name.value}-${version.value}.bin.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("log4j.properties", _*) => MergeStrategy.discard
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17",
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "net.liftweb" %% "lift-json" % "3.0.1"
    ))

lazy val core = (project in file(".")).
  settings(
    name := "qwery-core",
    organization := "com.github.ldaniels528",
    description := "A SQL-like query language for performing ETL",
    version := apiVersion,
    scalaVersion := appScalaVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-s3" % "1.11.129",
      "com.twitter" %% "bijection-avro" % "0.9.5",
      "log4j" % "log4j" % "1.2.17",
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "net.liftweb" %% "lift-json" % "3.0.1",
      //
      // Akka
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      //
      // Kafka/Zookeeper
      "org.apache.avro" % "avro" % "1.8.1",
      "org.apache.curator" % "curator-framework" % curatorVersion exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.curator" % "curator-test" % curatorVersion exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion
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
          <organization>com.github.ldaniels528</organization>
          <organizationUrl>https://github.com/ldaniels528</organizationUrl>
          <roles>
            <role>Project-Administrator</role>
            <role>Developer</role>
          </roles>
          <timezone>+7</timezone>
        </developer>
      </developers>
)

// loads the Scalajs-io root project at sbt startup
onLoad in Global := (Command.process("project cli", _: State)) compose (onLoad in Global).value
