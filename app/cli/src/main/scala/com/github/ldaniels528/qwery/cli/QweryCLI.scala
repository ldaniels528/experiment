package com.github.ldaniels528.qwery
package cli

import com.github.ldaniels528.qwery.AppConstants._
import com.github.ldaniels528.qwery.ops._

/**
  * Qwery CLI Application
  * @author lawrence.daniels@gmail.com
  */
object QweryCLI {
  private var alive: Boolean = _
  private val compiler = new QweryCompiler()
  private val tabular = new Tabular()
  private var ticker = 0L
  private val globalScope = RootScope()
  private val sb = new StringBuilder()

  /**
    * Starts the REPL application
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = start()

  /**
    * Starts the REPL
    */
  def start(): Unit = {
    alive = true
    println(welcome("CLI"))
    val commandPrompt = CommandPrompt()
    println(s"Using ${commandPrompt.getClass.getSimpleName} for input.")
    println("HINT: Press ENTER (twice) to run your query:")
    println()

    reset()
    while (alive) {
      try {
        // take in input until an empty line is received
        commandPrompt.readLine(prompt).map(_.trim) foreach { line =>
          sb.append(line).append('\n')

          if (line.isEmpty || line.equalsIgnoreCase("exit")) {
            val input = sb.toString().trim
            if (input.nonEmpty) interpret(input)
            reset()
          }
        }
      } catch {
        case e: Throwable =>
          System.err.println(e.getMessage)
          reset()
      }
    }

    sys.exit(0)
  }

  /**
    * Stops the REPL
    */
  def stop(): Unit = alive = false

  /**
    * Interprets a single or collection of SQL statement
    * @param input the SQL statements
    */
  private def interpret(input: String) = {
    input match {
      case "exit" => alive = false
      case sql =>
        println()
        for {
          op <- compiler.compileFully(sql)
          line <- tabular.transform(op.execute(globalScope))
        } println(line)
    }
  }

  /**
    * Displays the command prompt
    * @return the command prompt
    */
  private def prompt: String = {
    ticker += 1
    f"[$ticker%02d]> "
  }

  /**
    * Resets the command line buffer
    */
  private def reset(): Unit = {
    sb.clear()
    println()
  }

}
