package com.github.ldaniels528.qwery.etl

import java.io.File

import com.github.ldaniels528.qwery.QweryCompiler
import com.github.ldaniels528.qwery.ops.{CodeBlock, Executable}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Script Support
  * @author lawrence.daniels@gmail.com
  */
trait ScriptSupport {
  private lazy val log = LoggerFactory.getLogger(getClass)

  def compileScript(config: ETLConfig, name: String, scriptPath: String): Executable = {
    val scriptFile = new File(config.scriptsDir, scriptPath)
    log.info(s"[$name] Compiling script '${scriptFile.getName}'...")
    CodeBlock(operations = QweryCompiler.compileFully(Source.fromFile(scriptFile).mkString).toSeq)
  }

}
