package com.github.ldaniels528.qwery
package etl

import java.io.File

import com.github.ldaniels528.qwery.AppConstants._
import com.github.ldaniels528.qwery.actors.FileWatchingActor
import com.github.ldaniels528.qwery.actors.FileWatchingActor.StartFileWatch
import org.slf4j.LoggerFactory

import scala.util.Properties

/**
  * Qwery ETL Application
  * @author lawrence.daniels@gmail.com
  */
object QweryETL {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    println(welcome("ETL"))

    // get the home directory
    val baseDir = Properties.envOrNone(envHome).map(new File(_).getCanonicalFile)
      .getOrElse(throw new IllegalStateException(s"You must set environment variable '$envHome'"))

    // load the configuration
    val config = new ETLConfig(baseDir)

    // start a file watch for "$QWERY_HOME/incoming/"
    val fileWatcher = QweryActorSystem.createActor[FileWatchingActor]
    fileWatcher ! StartFileWatch(config.incomingDir, { file =>
      logger.info(s"New file: ${file.getAbsolutePath}")
    })
  }

}
