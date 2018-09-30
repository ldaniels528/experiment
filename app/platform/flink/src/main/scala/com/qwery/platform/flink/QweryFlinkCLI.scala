package com.qwery.platform.flink

import java.io.File

import com.qwery.language.SQLLanguageParser

/**
  * Qwery-Flink CLI
  * @author lawrence.daniels@gmail.com
  */
object QweryFlinkCLI {

  /**
    * For stand alone operation
    * @param args the given command line arguments
    */
  def main(args: Array[String]): Unit = {
    // check the command line arguments
    val (file, cliArgs) = args.toList match {
      case path :: _args => new File(path) -> _args
      case _ =>
        throw new IllegalArgumentException(s"java ${getClass.getName} <scriptFile> [<arg1> .. <argN>]")
    }

    // load the script and execute the code
    implicit val rc: FlinkQweryContext = new FlinkQweryContext()
    val sqlLanguageParser = new SQLLanguageParser {}
    val sql = sqlLanguageParser.parse(file)
    new FlinkQweryCompiler {}.compileAndRun(sql, cliArgs)
  }

}
