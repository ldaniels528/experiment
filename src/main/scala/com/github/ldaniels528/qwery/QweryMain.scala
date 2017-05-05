package com.github.ldaniels528.qwery

import com.github.ldaniels528.tabular.Tabular

/**
  * Qwery Main Application
  * @author lawrence.daniels@gmail.com
  */
object QweryMain {
  private val version = "0.1.2"
  private var alive = true
  private val compiler = new QweryCompiler()
  private val tabular = new Tabular()
  private var ticker = 0L

  def main(args: Array[String]): Unit = repl()

  /**
    * The interactive REPL
    */
  def repl(): Unit = {
    println(welcome)
    val sb = new StringBuilder()

    def reset(): Unit  = {
      sb.clear()
      print(prompt)
    }

    reset()
    while (alive) {
      try {
        // take in input until an empty line is received
        val line = Console.in.readLine().trim
        sb.append(line).append('\n')

        if (line.isEmpty || line.endsWith(";")) {
          val input = sb.toString().trim match {
            case s if s.endsWith(";") => s.dropRight(1)
            case s => s
          }
          if(input.nonEmpty) {
            val results = interpret(input)
            handleResults(results)
          }
          reset()
        }

      } catch {
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
      case query => compiler.compile(query).execute()
    }
  }

  private def prompt: String = {
    ticker += 1
    s"[$ticker]> "
  }

  private def welcome: String = {
    s"""
       | Qwery CLI v$version
       |         ,,,,,
       |         (o o)
       |-----oOOo-(_)-oOOo----
       |You can executes multi-line queries here like:
       |
       |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap, 'Summary Quote' FROM './companylist.csv'
       |WHERE Sector = 'Basic Industries'
       |LIMIT 5;
      """.stripMargin
  }

}
