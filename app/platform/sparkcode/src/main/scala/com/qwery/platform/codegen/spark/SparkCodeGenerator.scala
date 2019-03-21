package com.qwery.platform.codegen.spark

import java.io.{File, PrintWriter}

import com.qwery.language.SQLLanguageParser
import com.qwery.models._
import com.qwery.platform.spark.die
import com.qwery.util.ResourceHelper._
import com.qwery.util.StringHelper._

/**
  * Spark Code Generator
  * @author lawrence.daniels@gmail.com
  */
class SparkCodeGenerator(className: String,
                         packageName: String,
                         version: String = "1.0",
                         scalaVersion: String = "2.11.12",
                         sparkVersion: String = "2.3.2",
                         outputPath: String = "./gen-src") {

  /**
    * Generates the SBT build script
    * @return the [[File]] representing the generated build.sbt
    */
  def createBuildScript(): File = {
    writeToDisk(outputFile = new File(outputPath, s"built.sbt")) {
      s"""|name := "$className"
          |
          |version := "$version"
          |
          |scalaVersion := "$scalaVersion"
          |
          |libraryDependencies ++= Seq(
          |   "com.databricks" %% "spark-avro" % "4.0.0",
          |   "com.databricks" %% "spark-csv" % "1.5.0",
          |   "org.apache.spark" %% "spark-core" % "$sparkVersion",
          |   "org.apache.spark" %% "spark-hive" % "$sparkVersion",
          |   "org.apache.spark" %% "spark-sql" % "$sparkVersion",
          |   //
          |   // placeholder dependencies
          |   "com.qwery" %% "core" % "0.4.0" from "file://./lib/qwery-core_2.11-0.4.0.jar",
          |   "com.qwery" %% "platform-spark" % "0.4.0" from "file://./lib/qwery-platform-spark-code-gen_2.11-0.4.0.jar",
          |   "com.qwery" %% "language" % "0.4.0" from "file://./lib/qwery-language_2.11-0.4.0.jar"
          |)
          |""".stripMargin
    }
  }

  /**
    * Generates an executable class file
    * @param invokable the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  def createMainClass(invokable: Invokable): File = {
    // create the package directory
    val srcDir = new File(outputPath, "src/main/scala")
    val pkgDir = new File(srcDir, packageName.replaceAllLiterally(".", File.separator))
    if (!pkgDir.mkdirs() && !pkgDir.exists()) die(s"Failed to create the package directory (package '$packageName')")

    // write the class to disk
    writeToDisk(outputFile = new File(pkgDir, s"$className.scala")) {
      MainClass(className, packageName, invokable, imports = List(
        "com.qwery.models._",
        "com.qwery.models.StorageFormats._",
        TableManager.getClass.getName.replaceAllLiterally("$", ""),
        "org.apache.spark.SparkConf",
        "org.apache.spark.sql.functions._",
        "org.apache.spark.sql.types.StructType",
        "org.apache.spark.sql.DataFrame",
        "org.apache.spark.sql.Row",
        "org.apache.spark.sql.SparkSession",
        "org.slf4j.LoggerFactory"
      )).generate
    }
  }

  /**
    * Creates the SBT project structure
    * @return the [[File]] representing the `project` directory
    */
  def createProjectStructure(): File = {
    val projectDir = new File(outputPath, "project")
    if (!projectDir.mkdirs() && !projectDir.exists())
      die(s"Failed to create the SBT project directory ('${projectDir.getCanonicalPath}')")

    // generate the configuration files
    val configFiles = Seq(
      """addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")""" -> "assembly.sbt",
      """sbt.version=0.13.16""" -> "build.properties",
      """|addSbtPlugin("com.databricks" %% "sbt-databricks" % "0.1.5")
         |addSbtPlugin("com.typesafe.akka" % "akka-sbt-plugin" % "2.2.3")
         |""".stripMargin -> "plugins.sbt"
    )
    configFiles foreach { case (contents, path) =>
      writeToDisk(outputFile = new File(projectDir, path))(contents)
    }
    projectDir
  }

  /**
    * Generates a new SBT project
    * @param invokable the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  def generateProject(invokable: Invokable): File = {
    createProjectStructure()
    createBuildScript()
    createMainClass(invokable)
  }

  private def writeToDisk(outputFile: File)(contents: => String): File = {
    new PrintWriter(outputFile).use(_.println(contents))
    outputFile
  }

}

/**
  * Spark Code Generator Companion
  * @author lawrence.daniels@gmail.com
  */
object SparkCodeGenerator {

  /**
    * Creates a new Spark Code Generator
    * @param classNameWithPackage the given class and package names (e.g. "com.acme.spark.MyFirstSparkJob")
    * @return a [[SparkCodeGenerator]]
    * @example {{{ java com.qwery.platform.codegen.spark.SparkCodeGenerator ./samples/sql/companylist.sql com.acme.spark.MyFirstSparkJob }}}
    */
  def apply(classNameWithPackage: String): SparkCodeGenerator = {
    classNameWithPackage.lastIndexOfOpt(".").map(classNameWithPackage.splitAt) match {
      case Some((className, packageName)) => new SparkCodeGenerator(className, packageName)
      case None => new SparkCodeGenerator(classNameWithPackage, packageName = "com.qwery.examples")
    }
  }

  /**
    * For stand alone operation
    * @param args the given command line arguments
    */
  def main(args: Array[String]): Unit = {
    args.toList match {
      case sqlFile :: className :: genArgs =>
        val sql = SQLLanguageParser.parse(new File(sqlFile))
        SparkCodeGenerator(className).generateProject(sql)
      case _ =>
        die(s"java ${getClass.getName.replaceAllLiterally("$", "")} <scriptFile> <outputClass> [<arg1> .. <argN>]")
    }
  }

}
