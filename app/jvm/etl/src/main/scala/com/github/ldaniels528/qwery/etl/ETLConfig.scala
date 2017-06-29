package com.github.ldaniels528.qwery
package etl

import java.io.File
import java.util.UUID

import akka.actor.ActorRef
import com.github.ldaniels528.qwery.etl.actors.{FileManagementActor, QweryActorSystem, WorkflowManagementActor}
import com.github.ldaniels528.qwery.etl.events.ScheduledEvent
import com.github.ldaniels528.qwery.etl.triggers._
import com.github.ldaniels528.qwery.ops.Scope
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * ETL Configuration
  * @author lawrence.daniels@gmail.com
  */
class ETLConfig(val baseDir: File) {
  private lazy val log = LoggerFactory.getLogger(getClass)
  private val scheduledEvents = TrieMap[UUID, ScheduledEvent]()
  private val triggers = TrieMap[String, Trigger]()

  // define the processing directories
  val archiveDir: File = new File(baseDir, "archive")
  val configDir: File = new File(baseDir, "config")
  val failedDir: File = new File(baseDir, "failed")
  val inboxDir: File = new File(baseDir, "inbox")
  val scriptsDir: File = new File(baseDir, "scripts")
  val workDir: File = new File(baseDir, "work")

  // create the support actors
  val fileManager: ActorRef = QweryActorSystem.createActor(name = "fm", () => new FileManagementActor(this))
  val workflowManager: ActorRef = QweryActorSystem.createActor(name = "wm", () => new WorkflowManagementActor(this))

  // installation checks
  ensureSubdirectories(baseDir)

  /**
    * Adds the given scheduled event to the configuration
    * @param scheduledEvent the given [[ScheduledEvent scheduled event]]
    */
  def add(scheduledEvent: ScheduledEvent): Unit = scheduledEvents(scheduledEvent.uid) = scheduledEvent

  /**
    * Adds the given trigger to the configuration
    * @param trigger the given [[Trigger trigger]]
    */
  def add(trigger: Trigger): Unit = triggers(trigger.name) = trigger

  /**
    * ensures the existence of the processing sub-directories
    * @param baseDir the given base directory
    */
  private def ensureSubdirectories(baseDir: File) = {
    // make sure it exists
    if (!baseDir.exists || !baseDir.isDirectory)
      throw new IllegalStateException(s"Qwery Home directory '${baseDir.getAbsolutePath}' does not exist")

    val subDirectories = Seq(archiveDir, configDir, failedDir, inboxDir, scriptsDir, workDir)
    subDirectories.foreach { directory =>
      if (!directory.exists()) {
        log.info(s"Creating directory '${directory.getAbsolutePath}'...")
        directory.mkdir()
      }
    }
  }

  /**
    * Attempts to find a trigger that accepts the given file
    * @param scope the given [[Scope scope]]
    * @param file  the given file
    * @return an option of a [[Trigger]]
    */
  def lookupTrigger(scope: Scope, file: String): Option[Trigger] = triggers.find(_._2.accepts(scope, file)).map(_._2)

  /**
    * Loads the scheduled events found in ./config/scheduled-events.json
    */
  def loadScheduledEvents(): Unit = {
    val scheduledEvents = ScheduledEvent.loadScheduledEvents(this, configDir)
    scheduledEvents foreach { scheduledEvent =>
      add(scheduledEvent)
      scheduledEvent.update(this)
    }
  }

  /**
    * Loads the triggers found in ./config/triggers.json
    */
  def loadTriggers(): Unit = {
    FileTrigger.loadTriggers(this, configDir) foreach add
  }

}
