package com.github.ldaniels528.broadway.watcher

import java.io.File

import com.github.ldaniels528.qwery.AppConstants.{envHome, welcome}
import com.github.ldaniels528.qwery.actors.FileWatchingActor.{FileWatchCallback, Watch}
import com.github.ldaniels528.qwery.actors.{FileWatchingActor, QweryActorSystem}
import org.slf4j.LoggerFactory

import scala.util.Properties

/**
  * Broadway Watcher application
  * @author lawrence.daniels@gmail.com
  */
object WatcherApp {
  private val logger = LoggerFactory.getLogger(getClass)

  // create the actors
  private val fileWatcherActor = QweryActorSystem.createActor[FileWatchingActor](name = "fileWatcher")

  /**
    * Application entrypoint
    * @param args the given commandline arguments
    */
  def main(args: Array[String]): Unit = {
    val userWatcherConfig_? = args.headOption
    run(userWatcherConfig_?)
  }

  /**
    * Starts the worker process
    */
  def run(userWatcherConfig_? : Option[String]): Unit = {
    println(welcome("Watcher"))

    // get the home directory
    val baseDir = Properties.envOrNone(envHome).map(new File(_).getCanonicalFile)
      .getOrElse(throw new IllegalStateException(s"You must set environment variable '$envHome'"))

    // load the watcher's configuration
    val configFile = userWatcherConfig_?.map(new File(_)) getOrElse new File(new File(baseDir, "config"), "watcher.json")
    val config = WatcherConfig.load(configFile)

    // register the watch locations
    config.directories foreach { entry =>
      val directory = new File(entry.source)

      registerWatch(directory = directory) { file =>
        val targetDirectory = new File(entry.target)
        val targetFile = new File(targetDirectory, file.getName)
        logger.info(s"Moving '${file.getName}' from '${file.getParent}' to '${targetDirectory.getAbsolutePath}'...")
        file.renameTo(targetFile)
      }
    }

  }

  private def registerWatch(directory: File)(callback: FileWatchCallback): Unit = {
    val thePath = directory.getCanonicalFile.toPath
    logger.info(s"Watching directory '$thePath'...")
    fileWatcherActor ! Watch(thePath, callback)
  }

}
