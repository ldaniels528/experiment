package com.qwery.platform.sparksql.embedded

import java.io.File

import com.qwery.language.SQLLanguageParser
import com.qwery.util.ResourceHelper._
import com.qwery.util.StringHelper._

/**
  * Embedded Spark Job - Batch or Streaming
  * @author lawrence.daniels@gmail.com
  */
object EmbeddedSparkJob {
  private val defaultFileName = "/boot.sql"

  /**
    * For stand alone operation
    * @param args the given command line arguments
    */
  def main(args: Array[String]): Unit = {
    // create the Qwery runtime context
    implicit val rc: EmbeddedSparkContext = new EmbeddedSparkContext()

    // check the command line arguments
    args.toList match {
      case path :: jobArgs =>
        val sql = SQLLanguageParser.parse(new File(path))
        new EmbeddedSparkCompiler {}.compileAndRun(fileName = path, sql, args = jobArgs)
      case _ =>
        defaultFileName.asURL match {
          case Some(url) =>
            val sql = SQLLanguageParser.parse(url)
            new EmbeddedSparkCompiler {}.compileAndRun(fileName = defaultFileName, sql, args = Nil)
          case None =>
            die(s"java ${EmbeddedSparkJob.getObjectFullName} <scriptFile> [<arg1> .. <argN>]")
        }
    }
  }

}
