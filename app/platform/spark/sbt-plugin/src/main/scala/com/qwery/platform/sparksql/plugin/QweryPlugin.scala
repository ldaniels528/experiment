package com.qwery.platform
package sparksql
package plugin

import java.io.File

import com.qwery.platform.sparksql.generator.ApplicationSettings._
import com.qwery.platform.sparksql.generator.{ApplicationSettings, SparkJobGenerator}
import sbt.Keys._
import sbt._

/**
  * Qwery Spark-Code-Generation Plugin for SBT
  * @example
  * {{{
  * lazy val root = (project in file("."))
  *   .enablePlugins(QweryPlugin)
  *   .settings(
  *     scalaVersion := "2.12.8",
  *     version := "0.1",
  *     qweryClassName := "com.coxautoinc.maid.adbook.AdBookClientSparkJob",
  *     qweryInputFile := baseDirectory.value / "sql" / "adbook-client.sql",
  *     qweryOutputDirectory := baseDirectory.value / "src" / "main" / "scala",
  *     qweryClassOnly := Some(true),
  *     qweryTemplateFile := Some(baseDirectory.value / "src" / "main" / "resources" / "WtmSparkJobTemplate.txt"),
  *   )
  * }}}
  */
object QweryPlugin extends AutoPlugin {
  override val trigger: PluginTrigger = PluginTrigger.NoTrigger
  override val requires: Plugins = plugins.JvmPlugin

  object autoImport {
    lazy val qweryAppName = settingKey[Option[String]]("the Spark application name.")
    lazy val qweryAppVersion = settingKey[Option[String]]("the version identifier of your application.")
    lazy val qweryClassName = settingKey[String]("the fully qualified class name to generate.")
    lazy val qweryClassOnly = settingKey[Option[Boolean]]("indicates whether only a Scala class should be generated.")
    lazy val qweryDefaultDB = settingKey[Option[String]]("the default database.")
    lazy val qweryExtendsClass = settingKey[Option[String]]("the optional class the generated class should extend.")
    lazy val qweryInputFile = settingKey[File]("the path of the input .sql file.")
    lazy val qweryOutputDirectory = settingKey[File]("the path where the class files or SBT project will be generated.")
    lazy val qweryProperties = settingKey[Option[java.util.Properties]]("the optional Spark configuration properties   .")
    lazy val qweryScalaVersion = settingKey[Option[String]]("the Scala version the generated project will use.")
    lazy val qwerySparkVersion = settingKey[Option[String]]("the Apache Spark API version.")
    lazy val qweryTemplateFile = settingKey[Option[File]]("the optional template class to use in generating the Spark Job.")
    lazy val generate = taskKey[Unit]("Generates the class or project files.")
  }

  import autoImport._

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    qweryAppName := None,
    qweryAppVersion := None,
    qweryClassOnly := None,
    qweryDefaultDB := None,
    qweryExtendsClass := None,
    qweryProperties := None,
    qweryScalaVersion := None,
    qwerySparkVersion := None,
    qweryTemplateFile := None,
    generate := generateTask.value
  )

  private def generateTask = Def.task {
    val log = sLog.value

    // extract the class name and package from the fully qualified class name (e.g. "com.acme.CoyoteCrush")
    val (myClassName, myPackageName) = getClassAndPackageNames(qweryClassName.value)

    // create the application settings
    implicit val settings: ApplicationSettings = new ApplicationSettings.Builder()
      .withAppName(qweryAppName.value)
      .withAppVersion(qweryAppVersion.value)
      .withAppVersion(myClassName)
      .withClassOnly(qweryClassOnly.value)
      .withDefaultDB(qweryDefaultDB.value)
      .withExtendsClass(qweryExtendsClass.value)
      .withInputPath(qweryInputFile.value)
      .withOutputPath(qweryOutputDirectory.value)
      .withPackageName(myPackageName)
      .withProperties(qweryProperties.value)
      .withScalaVersion(qweryScalaVersion.value)
      .withSparkVersion(qwerySparkVersion.value)
      .withTemplateFile(qweryTemplateFile.value)
      .build

    // generate the project or class
    log.info(s"Generating class '${settings.fullyQualifiedClassName}'...")
    SparkJobGenerator.generate()
  }
}
