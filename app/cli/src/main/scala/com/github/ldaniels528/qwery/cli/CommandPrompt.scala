package com.github.ldaniels528.qwery.cli

import java.io.{BufferedReader, File, InputStreamReader}

import scala.tools.jline.console.ConsoleReader
import scala.tools.jline.console.history.FileHistory
import scala.util.Properties

/**
  * Command Prompt
  * @author lawrence.daniels@gmail.com
  */
trait CommandPrompt {

  def readLine(prompt: String): Option[String]

}

/**
  * Command Prompt
  * @author lawrence.daniels@gmail.com
  */
object CommandPrompt {

  def apply(): CommandPrompt = {
    Properties match {
      case p if p.isWin & System.console() == null => new DefaultCommandPrompt()
      case p if p.isWin => new WindowsCommandPrompt()
      case p if p.isLinux | p.isMac => new UNIXCommandPrompt()
      case _ => new DefaultCommandPrompt()
    }
  }
}

/**
  * UNIX Command Prompt
  * @author lawrence.daniels@gmail.com
  */
class UNIXCommandPrompt() extends CommandPrompt {
  // define the console reader
  private val consoleReader = new ConsoleReader()
  private val history = new FileHistory(new File(".qwery_history"))
  consoleReader.setExpandEvents(false)
  consoleReader.setHistory(history)

  override def readLine(prompt: String): Option[String] = {
    Option(consoleReader.readLine(prompt))
  }
}

/**
  * Windows Command Prompt
  * @author lawrence.daniels@gmail.com
  */
class WindowsCommandPrompt() extends CommandPrompt {

  override def readLine(prompt: String): Option[String] = {
    Console.print(prompt)
    Option(System.console().readLine())
  }
}

/**
  * Default Command Prompt
  * @author lawrence.daniels@gmail.com
  */
class DefaultCommandPrompt() extends CommandPrompt {
  private val reader = new BufferedReader(new InputStreamReader(System.in))

  override def readLine(prompt: String): Option[String] = {
    Console.print(prompt)
    Option(reader.readLine())
  }
}