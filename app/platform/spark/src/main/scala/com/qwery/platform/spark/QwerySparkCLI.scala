package com.qwery.platform.spark

import java.io.File

import com.qwery.language.SQLLanguageParser

/**
  * Qwery Spark CLI
  * @author lawrence.daniels@gmail.com
  */
object QwerySparkCLI {

  /**
    * For stand alone operation
    * @param args the given command line arguments
    */
  def main(args: Array[String]): Unit = {
    // check the command line arguments
    val (file, cliArgs) = args.toList match {
      case path :: _args => new File(path) -> _args
      case _ => die(s"java ${getClass.getName} <scriptFile> [<arg1> .. <argN>]")
    }

    // load the script and execute the code
    implicit val rc: SparkQweryContext = new SparkQweryContext()
    val sqlLanguageParser = new SQLLanguageParser {}
    val sql = sqlLanguageParser.parse(file)
    new SparkQweryCompiler {}.compileAndRun(sql, cliArgs)
  }

}
