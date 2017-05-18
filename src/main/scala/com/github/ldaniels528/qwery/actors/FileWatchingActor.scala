package com.github.ldaniels528.qwery.actors

import java.io.File
import java.nio.file.StandardWatchEventKinds.{ENTRY_CREATE, ENTRY_MODIFY}
import java.nio.file.{Paths, WatchKey}

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.qwery.QweryActorSystem
import com.github.ldaniels528.qwery.actors.FileWatchingActor.{FileWatchCallback, StartFileWatch}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * File Watching Actor
  * @author lawrence.daniels@gmail.com
  */
class FileWatchingActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case StartFileWatch(directory, action) =>
      registerWatch(directory, action)
    case message =>
      unhandled(message)
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
          callback(new File(directory, event.context().toString))
        })
      }
    }
  }

}

/**
  * File Watching Actor Companion
  * @author lawrence.daniels@gmail.com
  */
object FileWatchingActor {

  type FileWatchCallback = File => Unit

  case class StartFileWatch(directory: File, callback: FileWatchCallback)

}