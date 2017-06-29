package com.github.ldaniels528.transgress

import io.scalajs.nodejs.Process
import io.scalajs.util.OptionHelper._

/**
  * Environment Helper
  * @author lawrence.daniels@gmail.com
  */
object EnvironmentHelper {
  val AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
  val AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
  val TRANSGRESS_HOME = "TRANSGRESS_HOME"
  val TRANSGRESS_MASTER = "TRANSGRESS_MASTER"
  val TRANSGRESS_MONGODB = "TRANSGRESS_MONGODB"

  /**
    * Process configuration extensions
    * @param process the given [[Process process]]
    */
  final implicit class ProcessConfigExtensions(val process: Process) extends AnyVal {

    @inline
    def awsAccessKeyID: String = {
      process.env.get(AWS_ACCESS_KEY_ID).orDie(s"Environment variable $AWS_ACCESS_KEY_ID is not defined")
    }

    @inline
    def awsSecretAccessKey: String = {
      process.env.get(AWS_SECRET_ACCESS_KEY).orDie(s"Environment variable $AWS_SECRET_ACCESS_KEY is not defined")
    }

    @inline
    def homeDirectory: Option[String] = process.env.get(TRANSGRESS_HOME)

    @inline
    def master: Option[String] = process.env.get(TRANSGRESS_MASTER)

    /**
      * Attempts to returns the web application listen port
      * @return the option of the web application listen port
      */
    @inline
    def port: Option[String] = process.env.find(_._1.equalsIgnoreCase("port")).map(_._2)

    /**
      * Attempts to returns the database connection URL
      * @return the option of the database connection URL
      */
    @inline
    def dbConnect: Option[String] = process.env.find(_._1.equalsIgnoreCase(TRANSGRESS_MONGODB)).map(_._2)

    @inline
    def dbConnectOrDefault: String = dbConnect getOrElse "mongodb://localhost:27017/bourne"

  }

}
