package com.qwery.platform.flink

import java.io.File

import com.qwery.language.SQLLanguageParser
import com.qwery.util.ResourceHelper._

/**
  * Qwery Flink Job
  * @author lawrence.daniels@gmail.com
  */
object QweryFlinkJob {

  /**
    * For stand alone operation
    * @param args the given command line arguments
    */
  def main(args: Array[String]): Unit = {
    // create the Qwery runtime context
    implicit val rc: FlinkQweryContext = new FlinkQweryContext()

    // check the command line arguments
    args.toList match {
      case path :: jobArgs =>
        val sql = SQLLanguageParser.parse(new File(path))
        new FlinkQweryCompiler {}.compileAndRun(sql, args = jobArgs)
      case _ =>
        "/boot.sql".asURL match {
          case Some(url) =>
            val sql = SQLLanguageParser.parse(url)
            new FlinkQweryCompiler {}.compileAndRun(sql, args = Nil)
          case None =>
            die(s"java ${getClass.getName.replaceAllLiterally("$", "")} <scriptFile> [<arg1> .. <argN>]")
        }
    }
  }

}
