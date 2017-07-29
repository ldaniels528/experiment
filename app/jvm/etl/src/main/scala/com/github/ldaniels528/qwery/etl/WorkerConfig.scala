package com.github.ldaniels528.qwery.etl

import java.io.File

import com.github.ldaniels528.qwery.util.JSONSupport

import scala.io.Source

/**
  * Worker configuration
  * @param supervisor  the supervisor end point (e.g. "localhost:9000")
  * @param controlPort the client control (callback) port (e.g. "1337")
  */
case class WorkerConfig(supervisor: String, controlPort: String)

/**
  * Worker configuration
  * @author lawrence.daniels@gmail.com
  */
object WorkerConfig extends JSONSupport {

  /**
    * Loads the optional ETL worker (worker.json) configuration file
    * @param configFile the given [[File configuration file]]
    * @return the [[WorkerConfig processing configuration]]
    */
  def load(configFile: File): WorkerConfig = {
    if (configFile.exists()) {
      val workerConfigJs = parseJsonAs[WorkerConfigJSON](Source.fromFile(configFile).mkString)
      WorkerConfig(
        supervisor = workerConfigJs.supervisor getOrElse "localhost:9000",
        controlPort = workerConfigJs.controlPort getOrElse "1337"
      )
    }
    else WorkerConfig(supervisor = "localhost:9000", controlPort = "1337")
  }

  case class WorkerConfigJSON(supervisor: Option[String], controlPort: Option[String])

}
