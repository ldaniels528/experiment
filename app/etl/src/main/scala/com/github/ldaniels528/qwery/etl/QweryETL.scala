package com.github.ldaniels528.qwery
package etl

import java.io.File
import java.util.UUID

import com.github.ldaniels528.qwery.AppConstants._
import com.github.ldaniels528.qwery.etl.actors.FileManagementActor._
import com.github.ldaniels528.qwery.etl.actors.WorkflowManagementActor.ProcessFile
import com.github.ldaniels528.qwery.ops.RootScope
import org.slf4j.LoggerFactory

import scala.util.Properties

/**
  * Qwery ETL Application
  * @author lawrence.daniels@gmail.com
  */
object QweryETL {
  private val log = LoggerFactory.getLogger(getClass)

  /**
    * Main entry-point
    * @param args the given commandline arguments
    */
  def main(args: Array[String]): Unit = run()

  /**
    * Starts the ETL process
    */
  def run(): Unit = {
    println(welcome("ETL"))

    // get the home directory
    val baseDir = Properties.envOrNone(envHome).map(new File(_).getCanonicalFile)
      .getOrElse(throw new IllegalStateException(s"You must set environment variable '$envHome'"))

    // load the configuration
    val config = new ETLConfig(baseDir)
    config.loadTriggers()

    // define the root scope
    val rootScope = RootScope()

    // get references to the support actors
    val fileManager = config.fileManager
    val workflowManager = config.workflowManager

    // start a file watch for "$QWERY_HOME/inbox/"
    fileManager ! WatchFile(config.inboxDir, { file =>
      config.lookupTrigger(rootScope, file.getName) match {
        case Some(trigger) =>
          val pid = UUID.randomUUID()
          log.info(s"[$pid] Trigger '${trigger.name}' accepts '${file.getName}'")
          workflowManager ! ProcessFile(pid, file, trigger, rootScope)
        case None =>
          log.info(s"No trigger found for file '${file.getName}'")
      }
    })

    log.info("Hello.")
  }

}
