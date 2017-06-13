package com.github.ldaniels528.qwery.etl
package actors

import java.io.File
import java.nio.file.StandardWatchEventKinds.{ENTRY_CREATE, ENTRY_MODIFY}
import java.nio.file.{Paths, WatchKey}
import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.qwery.etl.actors.FileManagementActor._
import com.github.ldaniels528.qwery.util.DurationHelper._

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

/**
  * File Management Actor
  * @author lawrence.daniels@gmail.com
  */
class FileManagementActor(config: ETLConfig) extends Actor with ActorLogging {
  private val watchedFiles = TrieMap[String, WatchedFile]()
  private implicit val dispatcher = context.dispatcher

  override def receive: Receive = {
    case ArchiveFile(file, compress) => storeFile(file, compress)
    case MoveFile(file, directory) => moveFile(file, directory)
    case WatchFile(directory, callback) => registerWatch(directory, callback)
    case message => unhandled(message)
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

  private def registerWatch(directory: File, callback: FileWatchCallback) = {
    val path = Paths.get(directory.getAbsolutePath)
    val watcher = path.getFileSystem.newWatchService()
    Seq(ENTRY_CREATE, ENTRY_MODIFY) foreach (path.register(watcher, _))

    var watchKey: Option[WatchKey] = None
    QweryActorSystem.scheduler.schedule(0.seconds, 1.second) {
      if (watchKey.isEmpty) watchKey = Option(watcher.poll())
      else {
        watchKey.foreach(_.pollEvents().asScala.foreach { event =>
          val file = new File(directory, event.context().toString)
          if (!watchedFiles.contains(file.getCanonicalPath)) {
            callback(file)
          }
        })
      }
    }

    // process any pre-existing files
    Option(directory.listFiles()) foreach { files =>
      if (files.nonEmpty) {
        log.info(s"Processing ${files.length} pre-existing files...")
        for (file <- files) callback(file)
      }
    }
  }

  private def storeFile(fileToStore: File, compress: Boolean): Boolean = {
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

  case class WatchFile(directory: File, callback: FileWatchCallback)

  case class WatchedFile(file: File)

}