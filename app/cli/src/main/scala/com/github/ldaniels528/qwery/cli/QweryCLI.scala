package com.github.ldaniels528.qwery
package cli

import com.github.ldaniels528.qwery.AppConstants._
import com.github.ldaniels528.qwery.ops._

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
    println(welcome("CLI"))
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

          if (line.isEmpty || line.equalsIgnoreCase("exit") || line.contains(";")) {
            val input = sb.toString().trim
            if (input.nonEmpty) interpret(input)
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

  private def interpret(input: String) = {
    input match {
      case "exit" => alive = false
      case sql =>
        for {
          op <- compiler.compileFully(sql)
          line <- tabular.transform(op.execute(globalScope))
        } println(line)
    }
  }

  private def prompt: String = {
    ticker += 1
    f"[$ticker%02d]> "
  }

}
