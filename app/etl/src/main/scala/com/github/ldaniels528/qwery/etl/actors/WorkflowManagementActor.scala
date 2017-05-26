package com.github.ldaniels528.qwery.etl.actors

import java.io.File
import java.util.UUID

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.qwery.devices.DeviceHelper._
import com.github.ldaniels528.qwery.etl.ETLConfig
import com.github.ldaniels528.qwery.etl.actors.FileManagementActor.ArchiveFile
import com.github.ldaniels528.qwery.etl.actors.ResourceReadingActor.{DataReceived, EOF}
import com.github.ldaniels528.qwery.etl.actors.WorkflowManagementActor._
import com.github.ldaniels528.qwery.etl.triggers.Trigger
import com.github.ldaniels528.qwery.etl.workflows.Workflow
import com.github.ldaniels528.qwery.ops.{LocalScope, ResultSet, Scope, Variable}

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}

/**
  * Workflow Management Actor
  * @author lawrence.daniels@gmail.com
  */
class WorkflowManagementActor(config: ETLConfig) extends Actor with ActorLogging {
  private implicit val dispatcher = context.dispatcher

  override def receive: Receive = {
    case DataReceived(pid, row) => jobs.get(pid) foreach (_.write(row))
    case EOF(pid, _, statistics) => jobs.remove(pid) foreach (_.close(statistics))
    case ProcessFile(pid, file, trigger, scope) => processFile(pid, file, trigger, scope)
    case message => unhandled(message)
  }

  /**
    * Moves the file to the failed directory
    * @param pid  the given process ID
    * @param file the file to move
    * @return true, if the file was successfully moved
    */
  private def moveToFailed(pid: UUID, file: File): Option[File] = moveToDirectory(pid, file, config.failedDir)

  /**
    * Moves the file to the work directory
    * @param pid  the given process ID
    * @param file the file to move
    * @return true, if the file was successfully moved
    */
  private def moveToWork(pid: UUID, file: File): Option[File] = moveToDirectory(pid, file, config.workDir)

  /**
    * Moves the file to the directory
    * @param pid  the given process ID
    * @param file the file to move
    * @return true, if the file was successfully moved
    */
  private def moveToDirectory(pid: UUID, file: File, directory: File): Option[File] = {
    if (file.exists()) {
      val targetDirectory = new File(directory, pid.toString)
      val targetFile = new File(targetDirectory, file.getName)
      val renamed = targetDirectory.mkdirs() && file.renameTo(targetFile)
      if (renamed) Some(targetFile) else {
        log.warning(s"[$pid] File '${file.getAbsolutePath}' could not be moved")
        None
      }
    } else {
      log.warning(s"[$pid] File '${file.getCanonicalPath}' does not exist")
      None
    }
  }

  private def processFile(pid: UUID, file: File, trigger: Trigger, rootScope: Scope): Unit = {
    log.info(s"[$pid] Preparing to process Inbox:'${file.getName}'")
    moveToWork(pid, file) match {
      case Some(workFile) =>
        log.info(s"Processing file '${workFile.getAbsolutePath}' using '${trigger.name}'...")

        // set a new scope with processing variables
        val scope = LocalScope(rootScope, row = Nil)
        scope += Variable("work.file.base", Option(workFile.getBaseName))
        scope += Variable("work.file.name", Option(workFile.getName))
        scope += Variable("work.file.path", Option(workFile.getCanonicalPath))
        scope += Variable("work.file.size", Option(workFile).map(_.getSize))
        scope += Variable("work.path", Option(workFile.getParentFile))

        // start the workflow
        val startTime = System.currentTimeMillis()
        Try(trigger.execute(scope, workFile.getName)) match {
          case Success(ResultSet(_, statistics)) =>
            val elapsedTime = System.currentTimeMillis() - startTime
            log.info(s"[$pid] Process completed successfully in $elapsedTime msec")
            statistics.foreach(stats => log.info(s"[$pid] $stats"))
            config.fileManager ! ArchiveFile(workFile.getParentFile)
          case Failure(e) =>
            log.error(s"[$pid] Process failed for '${file.getName}': ${e.getMessage}", e)
            moveToFailed(pid, workFile)
        }
      case None =>
        log.error(s"[$pid] Failed to move '${file.getName}' from the Inbox to the Work area")
    }
  }

}

/**
  * Workflow Management Actor Companion
  * @author lawrence.daniels@gmail.com
  */
object WorkflowManagementActor {
  private val jobs = TrieMap[UUID, Workflow]()

  case class ProcessFile(pid: UUID, file: File, trigger: Trigger, scope: Scope)

}
