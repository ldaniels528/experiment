package com.github.ldaniels528.qwery.etl
package actors

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.qwery.actors.QweryActorSystem
import com.github.ldaniels528.qwery.etl.actors.FileManagementActor._
import com.github.ldaniels528.qwery.util.DurationHelper._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

/**
  * File Management Actor
  * @author lawrence.daniels@gmail.com
  */
class FileManagementActor(config: ETLConfig) extends Actor with ActorLogging {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val dispatcher = context.dispatcher

  override def receive: Receive = {
    case ArchiveFile(file, compress) => storeFile(file, compress)
    case MoveFile(file, directory) => moveFile(file, directory)
    case message =>
      log.warning(s"Unexpected message '$message' (${Option(message).map(_.getClass.getName).orNull})")
      unhandled(message)
  }

  /**
    * Moves the file to the directory
    * @param file      the file to move
    * @param directory the directory for which to move the file
    * @return true, if the file was successfully moved
    */
  private def moveFile(file: File, directory: File): Boolean = {
    log.info(s"Moving '${file.getName}' to '${directory.getAbsolutePath}'")
    if (file.exists()) {
      val targetFile = new File(directory, file.getName)
      val renamed = file.renameTo(targetFile)
      if (!renamed) log.warning(s"File '${file.getAbsolutePath}' could not be moved")
      renamed
    }
    else {
      log.warning(s"File '${file.getCanonicalPath}' does not exist")
      false
    }
  }

  private def storeFile(fileToStore: File, compress: Boolean) = {
    fileToStore match {
      case file if file.isFile && file.lastModified() > System.currentTimeMillis() - 15.seconds =>
        log.info(s"File '${file.getName}' is not at least 15 seconds old")
        QweryActorSystem.scheduler.scheduleOnce(delay = 5.seconds) {
          self ! ArchiveFile(fileToStore, compress)
        }
        false

      case file if file.exists() =>
        val ts = new Date()
        val year = new SimpleDateFormat("yyyy").format(ts)
        val month = new SimpleDateFormat("MM").format(ts)
        val day = new SimpleDateFormat("dd").format(ts)
        val hhmmss = new SimpleDateFormat("hhmmss").format(ts)
        val archiveFile = Seq(year, month, day, hhmmss, fileToStore.getName).foldLeft(config.archiveDir) { (directory, name) =>
          new File(directory, name)
        }
        log.info(s"Moving '${fileToStore.getName}' to '${archiveFile.getAbsolutePath}'")
        val archiveDirectory = archiveFile.getParentFile
        if (!archiveDirectory.exists()) archiveDirectory.mkdirs()
        fileToStore.renameTo(archiveFile)

      case file =>
        log.warning(s"File '${file.getCanonicalPath}' does not exist")
        false
    }
  }

}

/**
  * File Management Actor Companion
  * @author lawrence.daniels@gmail.com
  */
object FileManagementActor {

  type FileWatchCallback = File => Unit

  case class ArchiveFile(file: File, compress: Boolean = false)

  case class MoveFile(file: File, directory: File)


}