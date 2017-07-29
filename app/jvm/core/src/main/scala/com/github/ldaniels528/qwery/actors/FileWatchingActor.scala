package com.github.ldaniels528.qwery.actors

import java.io.File
import java.nio.file.StandardWatchEventKinds.{ENTRY_CREATE, ENTRY_MODIFY, OVERFLOW}
import java.nio.file.{FileSystems, Path}

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.qwery.actors.FileWatchingActor._
import com.github.ldaniels528.qwery.util.DurationHelper._
import com.github.ldaniels528.qwery.util.FileHelper.getFiles

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * File Watching Actor
  * @author lawrence.daniels@gmail.com
  */
class FileWatchingActor() extends Actor with ActorLogging {
  private val watchedFiles = TrieMap[String, WatchedFile]()
  private val watchService = FileSystems.getDefault.newWatchService

  def alive: Boolean = true

  override def receive: Receive = {
    case Watch(path, callback: FileWatchCallback) =>
      path.register(watchService, ENTRY_CREATE, ENTRY_MODIFY)
      import context.dispatcher

      // process any pre-existing files
      Option(getFiles(path.toFile)) foreach { files =>
        if (files.nonEmpty) {
          log.info(s"Processing ${files.length} pre-existing files...")
          files foreach (watch(_, callback))
        }
      }

      Future {
        while (alive) {
          val watchKey = watchService.take()
          watchKey.pollEvents.asScala foreach { event =>
            event.kind() match {
              case OVERFLOW =>
                val files = getFiles(path.toFile)
                files foreach (watch(_, callback))
              case _ =>
                val context = event.context.asInstanceOf[Path]
                val file = path.resolve(context).toFile
                watch(file, callback)
            }
          }

          // reset the key
          val valid = watchKey.reset
          if (!valid) log.error("Watch Key has been unregistered")
        }
        log.info("Dead.")
      }

    case message =>
      log.warning(s"Unhandled message $message")
      unhandled(message)
  }

  private def watch(file: File, callback: FileWatchCallback)(implicit ec: ExecutionContext): Unit = {
    val path = file.getCanonicalPath
    val watchedFile = watchedFiles.getOrElseUpdate(path, WatchedFile(file))
    if (watchedFile.isReady(15.seconds)) {
      Try(callback(file))
      watchedFiles.remove(path)
    }
    else {
      val sleepDuration = Math.max(15.seconds.toMillis - watchedFile.age.toMillis, 0).seconds
      if (sleepDuration > 0) log.info(s"'$path' is too young (${watchedFile.age})")
      QweryActorSystem.scheduler.scheduleOnce(sleepDuration)(watch(file, callback))
    }
  }

}

/**
  * File Watching Actor Companion
  * @author lawrence.daniels@gmail.com
  */
object FileWatchingActor {

  type FileWatchCallback = File => Unit

  case class Watch(directory: Path, callback: FileWatchCallback)

  case class WatchedFile(file: File) {
    private var length: Long = _
    private var mtime: Long = _

    def age: FiniteDuration = System.currentTimeMillis() - file.lastModified()

    def isReady(minAge: FiniteDuration): Boolean = {
      val ok = file.length == length && file.lastModified == mtime && age >= minAge
      if (!ok) {
        length = file.length()
        mtime = file.lastModified()
      }
      ok
    }

  }

}