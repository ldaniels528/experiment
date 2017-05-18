package com.github.ldaniels528.qwery.triggers

import java.io.File
import java.nio.file.StandardWatchEventKinds.{ENTRY_CREATE, ENTRY_MODIFY}
import java.nio.file.{Path, Paths, WatchService}

import com.github.ldaniels528.qwery.triggers.LocalFileTrigger.{QueuedFile, Registration}
import com.github.ldaniels528.qwery.util.DurationHelper._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

/**
  * Local File Trigger
  * @author lawrence.daniels@gmail.com
  */
class LocalFileTrigger {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val registrations = TrieMap[File, Registration[_]]()
  private[this] val queue = TrieMap[File, QueuedFile[_]]()

  def register[A](watcherName: String, directory: File)(callback: File => A): Registration[_] = {
    val _directory = directory.getCanonicalFile
    registrations.getOrElseUpdate(_directory, {
      logger.info(s"$watcherName is watching for new files in '${_directory.getAbsolutePath}'...")
      val path = Paths.get(_directory.getAbsolutePath)
      val watcher = path.getFileSystem.newWatchService()
      val registration = Registration(watcherName, _directory, path, watcher, callback)
      processPreExistingFiles(_directory, registration)
      registration
    })
  }

  /**
    * Recursively schedules all files found in the given directory for processing
    * @param directory    the given directory
    * @param registration the given registration
    */
  private def processPreExistingFiles[A](directory: File, registration: Registration[A]) {
    Option(directory.listFiles) foreach { files =>
      logger.info(s"Processing ${files.length} pre-existing files...")
      files foreach {
        case f if f.isFile => notifyWhenReady(f, registration)
        case d if d.isDirectory => processPreExistingFiles(d, registration)
        case _ =>
      }
    }
  }

  /**
    * notifies the caller when the file is ready
    * @param file         the given [[File]]
    * @param registration the given registration
    */
  private def notifyWhenReady[A](file: File, registration: Registration[A]) {
    queue.putIfAbsent(file, QueuedFile(file, registration))
    ()
  }

}

/**
  * Local File Trigger Companion
  * @author lawrence.daniels@gmail.com
  */
object LocalFileTrigger extends LocalFileTrigger {

  /**
    * Represents a queued file
    */
  case class QueuedFile[A](file: File, registration: Registration[A]) {
    // capture the file's initial state
    private var state0 = FileChangeState(file.lastModified(), file.length())

    /**
      * Attempts to determine whether the file is complete or not
      * @return true, if the file's size or last modified time hasn't changed in [up to] 10 seconds
      */
    def isReady: Boolean = {
      if (state0.elapsed < 1.second) false
      else {
        // get the last modified time and file size
        val state1 = FileChangeState(file.lastModified(), file.length())

        // has the file changed?
        val unchanged = state0.time == state1.time && state0.size == state1.size
        if (!unchanged) {
          state0 = state1.copy(lastChange = System.currentTimeMillis())
        }

        // return the result
        state0.elapsed >= 15.seconds && unchanged
      }
    }
  }

  case class FileChangeState(time: Long, size: Long, lastChange: Long = System.currentTimeMillis()) {
    def elapsed: Long = System.currentTimeMillis() - lastChange
  }

  /**
    * Represents a file watching registration
    * @param watcherName the given unique registration ID
    * @param directory   the directory to watch
    * @param path        the path to watch
    * @param watcher     the [[WatchService watch service]]
    * @param callback    the callback
    * @tparam A the callback return type
    */
  case class Registration[A](watcherName: String, directory: File, path: Path, watcher: WatchService, callback: File => A) {
    Seq(ENTRY_CREATE, ENTRY_MODIFY) foreach (path.register(watcher, _))
  }

}