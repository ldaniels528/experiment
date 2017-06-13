package com.github.ldaniels528.transgress

import io.scalajs.nodejs.path.Path
import io.scalajs.npm.moment.Moment

import scala.scalajs.js

/**
  * Represents a generic Transgress configuration
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait TransgressConfig extends js.Object {
  var baseDirectory: js.UndefOr[String] = js.native
}

/**
  * TransgressConfig Companion
  * @author lawrence.daniels@gmail.com
  */
object TransgressConfig {
  private val slash = Path.delimiter

  /**
    * Watcher Config Methods
    * @param config the given [[TransgressConfig watcher configuration]]
    */
  final implicit class WatcherConfigMethods(val config: TransgressConfig) extends AnyVal {

    @inline
    def archiveDirectory: String =  Path.join(config.baseDirectory.orNull, "archive")

    @inline
    def archiveFile(file: String): js.UndefOr[String] = getArchivePath(file, archiveDirectory)

    @inline
    def incomingDirectory: String = Path.join(config.baseDirectory.orNull, "incoming")

    @inline
    def triggerDirectory: String = Path.join(config.baseDirectory.orNull, "config", "triggers")

    @inline
    def triggerFile(file: String): js.UndefOr[String] = getPath(s"$file.json", triggerDirectory)

    @inline
    def workDirectory: String = Path.join(config.baseDirectory.orNull, "work")

    @inline
    def workFile(file: String): js.UndefOr[String] = getPath(file, workDirectory)

    @inline
    def workflowDirectory: String = Path.join(config.baseDirectory.orNull, "config", "workflows")

    @inline
    def workflowFile(file: String): js.UndefOr[String] = getPath(s"$file.json", workflowDirectory)

    /**
      * Returns the archive path for the given file
      * @param file      the given file
      * @param directory the given archive directory
      * @return the archive path (e.g. "./example/archive/20170327/181910/customers.csv")
      */
    private def getArchivePath(file: String, directory: String) = {
      val path = Path.parse(file)
      val moment = Moment()
      val date = moment.format("YYYYMMDD")
      val time = moment.format("HHmmss")
      for {
        base <- path.base
      } yield Path.join(directory, date, time, base).replaceAllLiterally(slash * 2, slash)
    }

    /**
      * Returns the sub-directory path for the given file
      * @param file      the given file
      * @param directory the given base directory
      * @return the path (e.g. "./example/workflows/customers_workflow.json")
      */
    private def getPath(file: String, directory: String) = {
      Path.parse(file).base.map(base => Path.join(directory, base).replaceAllLiterally(slash * 2, slash))
    }

  }

}