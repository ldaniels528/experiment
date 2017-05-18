package com.github.ldaniels528.qwery
package cli

import com.github.ldaniels528.qwery.AppConstants._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular._

/**
  * Qwery CLI Application
  * @author lawrence.daniels@gmail.com
  */
object QweryCLI {
  private var alive = true
  private val compiler = new QweryCompiler()
  private val tabular = new Tabular()
  private var ticker = 0L
  private val globalScope = RootScope()

  def main(args: Array[String]): Unit = repl()

  /**
    * The interactive REPL
    */
  def repl(): Unit = {
    println(welcome)
    val commandPrompt = CommandPrompt()
    println(s"Using ${commandPrompt.getClass.getSimpleName} for input.")

    val sb = new StringBuilder()

    def reset(): Unit = {
      sb.clear()
      println()
    }

    reset()
    while (alive) {
      try {
        // take in input until an empty line is received
        commandPrompt.readLine(prompt).map(_.trim) foreach { line =>
          sb.append(line).append('\n')

          if (line.isEmpty || line.endsWith(";")) {
            val input = sb.toString().trim match {
              case s if s.endsWith(";") => s.dropRight(1)
              case s => s
            }
            if (input.nonEmpty) {
              val results = interpret(input)
              println()
              handleResults(results)
            }
            reset()
          }
        }
      }
      catch {
        case e: Throwable =>
          System.err.println(e.getMessage)
          reset()
      }
    }

    sys.exit(0)
  }

  def die(): Unit = alive = false

  private def handleResults(results: Any): Unit = {
    results match {
      case results: ResultSet => tabular.transform(results) foreach println
      case _: Unit => println("Ok")
      case _ =>
    }
  }

  private def interpret(input: String) = {
    input match {
      case "exit" => alive = false
      case query => compiler.compile(query).execute(globalScope)
    }
  }

  private def prompt: String = {
    ticker += 1
    s"[$ticker]> "
  }

  private def welcome: String = {
    s"""
       | Qwery CLI v$Version
       |         ,,,,,
       |         (o o)
       |-----oOOo-(_)-oOOo-----
       |You can executes multi-line queries here like:
       |
       |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap, `Summary Quote` FROM './companylist.csv'
       |WHERE Sector = 'Basic Industries'
       |LIMIT 5;
      """.stripMargin
  }

}
