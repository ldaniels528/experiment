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
class SparkCodeGenerator(className: String, packageName: String, outputPath: String = "./gen-src") {

  def generate(invokable: Invokable): File = {
    val classDefinition =
      MainClass(className, packageName, invokable, imports = List(
        "com.qwery.models._",
        "com.qwery.models.StorageFormats._",
        TableManager.getClass.getName.replaceAllLiterally("$", ""),
        "org.apache.spark.SparkConf",
        "org.apache.spark.sql.types.StructType",
        "org.apache.spark.sql.DataFrame",
        "org.apache.spark.sql.Row",
        "org.apache.spark.sql.SparkSession",
        "org.slf4j.LoggerFactory"
      )).generate

    // create the package directory
    val pkgDir = new File(outputPath, packageName.replaceAllLiterally(".", File.separator))
    if (!pkgDir.mkdirs() && !pkgDir.exists()) die(s"Failed to create the package directory (package '$packageName')")

    // write the class to disk
    val outputFile = new File(pkgDir, s"$className.scala")
    new PrintWriter(outputFile).use(_.println(classDefinition))
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
        SparkCodeGenerator(className).generate(sql)
      case _ =>
        die(s"java ${getClass.getName.replaceAllLiterally("$", "")} <scriptFile> <outputClass> [<arg1> .. <argN>]")
    }
  }

}
