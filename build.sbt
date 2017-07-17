import org.scalajs.sbtplugin.ScalaJSPlugin
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt.Keys._
import sbt.Project.projectToRef
import sbt._

import scala.language.postfixOps

val appVersion = "0.3.9pre"
val scalaJvmVersion = "2.11.11"
val scalaJsVersion = "2.12.2"
val scalaJsIOVersion = "0.4.0"

val akkaVersion = "2.5.2"
val jacksonVersion = "2.8.9"
val kafkaVersion = "0.10.2.1"
val playVersion = "2.6.0"
val playWsStandaloneVersion = "1.0.0"
val scalaTestVersion = "3.0.1"
val slf4jVersion = "1.7.25"
val sparkVersion = "2.1.1"

/////////////////////////////////////////////////////////////////////////////////
//      Scala (JVM)
/////////////////////////////////////////////////////////////////////////////////

lazy val testDependencies = Seq(
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  ))

lazy val root = (project in file("./app/jvm/bundle")).
  aggregate(cli, etl).
  dependsOn(cli, etl).
  settings(publishingSettings: _*).
  settings(
    name := "qwery-bundle",
    organization := "io.scalajs",
    description := "Broadway Application Bundle",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true
  )

lazy val jvm_core = (project in file("./app/jvm/core")).
  settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "qwery-core",
    organization := "io.scalajs",
    description := "A SQL-like query language for performing ETL",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-s3" % "1.11.129",
      "commons-io" % "commons-io" % "2.5",
      "mysql" % "mysql-connector-java" % "5.1.42",
      "net.liftweb" %% "lift-json" % "3.0.1",
      //
      // Avro
      "com.twitter" %% "bijection-avro" % "0.9.5",
      "org.apache.avro" % "avro" % "1.8.2",
      //
      // Jackson
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      //
      // Kafka
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      //
      // Spark
      "com.databricks" %% "spark-avro" % "3.2.0",
      "com.databricks" %% "spark-csv" % "1.5.0",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      //
      // SLF4J
      "log4j" % "log4j" % "1.2.17",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
      //
      // TypeSafe
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.play" %% "play-json" % "2.6.0",
      "com.typesafe.play" %% "play-ahc-ws-standalone" % playWsStandaloneVersion,
      "com.typesafe.play" %% "play-ws-standalone-json" % playWsStandaloneVersion
    ))

lazy val cli = (project in file("./app/jvm/cli")).
  aggregate(jvm_core).
  dependsOn(jvm_core).
  settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "qwery-cli",
    organization := "io.scalajs",
    description := "Qwery CLI Application",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
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
      //
      // General Dependencies
      "org.scala-lang" % "jline" % "2.11.0-M3",
      //
      // SLF4J
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
    ))

lazy val etl = (project in file("./app/jvm/etl")).
  aggregate(jvm_core).
  dependsOn(jvm_core).
  settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "qwery-etl",
    organization := "io.scalajs",
    description := "Broadway ETL Worker/Orchestration Server",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
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
      //
      // SLF4J
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
    ))

lazy val spark = (project in file("./app/jvm/spark")).
  aggregate(jvm_core).
  dependsOn(jvm_core).
  settings(publishingSettings: _*).
  settings(testDependencies: _*).
  settings(
    name := "qwery-spark",
    organization := "io.scalajs",
    description := "Qwery Spark Integration",
    version := appVersion,
    scalaVersion := scalaJvmVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    autoCompilerPlugins := true,
    coverageEnabled := true,
    libraryDependencies ++= Seq(
      //
      // SLF4J
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
    ))

/////////////////////////////////////////////////////////////////////////////////
//      Scala.js (JavaScript/Node)
/////////////////////////////////////////////////////////////////////////////////

lazy val copyJS = TaskKey[Unit]("copyJS", "Copy JavaScript files to root directory")
copyJS := {
  val out_dir = baseDirectory.value
  val supervisor_dir = out_dir / "app" / "js" / "supervisor" / "target" / s"scala-${scalaJsVersion.take(4)}"
  val files1 = Seq("", ".map") map ("broadway-supervisor-fastopt.js" + _) map (s => (supervisor_dir / s, out_dir / s))
  IO.copy(files1, overwrite = true)
}

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-feature", "-deprecation"),
  scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
  scalaVersion := scalaJsVersion,
  autoCompilerPlugins := true,
  relativeSourceMaps := true,
  homepage := Some(url("https://github.com/ldaniels528/broadway.js")),
  resolvers += Resolver.sonatypeRepo("releases"))

lazy val appSettings = Seq(
  scalacOptions ++= Seq("-feature", "-deprecation"),
  scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
  scalaVersion := scalaJsVersion,
  scalaJSModuleKind := ModuleKind.CommonJSModule,
  autoCompilerPlugins := true,
  relativeSourceMaps := true,
  homepage := Some(url("https://github.com/ldaniels528/broadway.js")),
  resolvers += Resolver.sonatypeRepo("releases"))

lazy val testJsDependencies = Seq(
  libraryDependencies ++= Seq(
    "org.scalatest" %%% "scalatest" % scalaTestVersion % "test"
  ))

lazy val common_core = (project in file("./app/js/common/core"))
  .enablePlugins(ScalaJSPlugin)
  .settings(commonSettings: _*)
  .settings(testJsDependencies: _*)
  .settings(
    name := "broadway-common-core",
    organization := "com.github.ldaniels528",
    version := appVersion,
    libraryDependencies ++= Seq(
      "io.scalajs" %%% "core" % scalaJsIOVersion
    ))

lazy val common_cli = (project in file("./app/js/common/cli"))
  .dependsOn(common_core)
  .enablePlugins(ScalaJSPlugin)
  .settings(commonSettings: _*)
  .settings(testJsDependencies: _*)
  .settings(
    name := "broadway-common-cli",
    organization := "com.github.ldaniels528",
    version := appVersion,
    libraryDependencies ++= Seq(
      "io.scalajs" %%% "core" % scalaJsIOVersion,
      "io.scalajs" %%% "nodejs" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "moment" % scalaJsIOVersion
    ))

lazy val client = (project in file("./app/js/client"))
  .aggregate(common_core)
  .dependsOn(common_core)
  .enablePlugins(ScalaJSPlugin)
  .settings(commonSettings: _*)
  .settings(testJsDependencies: _*)
  .settings(
    name := "broadway-web-client",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaJSUseMainModuleInitializer := true,
    libraryDependencies ++= Seq(
      "io.scalajs" %%% "core" % scalaJsIOVersion,
      "io.scalajs" %%% "dom-html" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "angular" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "angular-ui-router" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "angularjs-toaster" % scalaJsIOVersion
    ))

lazy val supervisor = (project in file("./app/js/supervisor"))
  .aggregate(common_core, client, common_cli)
  .dependsOn(common_core, common_cli)
  .enablePlugins(ScalaJSPlugin)
  .settings(appSettings: _*)
  .settings(testJsDependencies: _*)
  .settings(
    name := "broadway-supervisor",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaJSUseMainModuleInitializer := true,
    libraryDependencies ++= Seq(
      "io.scalajs" %%% "core" % scalaJsIOVersion,
      "io.scalajs" %%% "nodejs" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "body-parser" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "express" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "express-fileupload" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "express-ws" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "mongodb" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "request" % scalaJsIOVersion,
      "io.scalajs.npm" %%% "splitargs" % scalaJsIOVersion
    ))

lazy val webapp = (project in file("."))
  .aggregate(client, supervisor)
  .dependsOn(client, supervisor)
  .enablePlugins(ScalaJSPlugin)
  .settings(appSettings: _*)
  .settings(testJsDependencies: _*)
  .settings(
    name := "broadway-webapp",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaVersion := scalaJsVersion,
    relativeSourceMaps := true,
    compile in Compile <<=
      (compile in Compile) dependsOn (fastOptJS in(client, Compile)),
    ivyScala := ivyScala.value map (_.copy(overrideScalaVersion = true)),
    Seq(scalaJSUseMainModuleInitializer, fastOptJS, fullOptJS) map { packageJSKey =>
      crossTarget in(client, Compile, packageJSKey) := baseDirectory.value / "public" / "javascripts"
    })

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

// add the alias
addCommandAlias("fastOptJSCopy", ";fastOptJS;copyJS")

// loads the Scalajs-io root project at sbt startup
onLoad in Global := (Command.process("project root", _: State)) compose (onLoad in Global).value
