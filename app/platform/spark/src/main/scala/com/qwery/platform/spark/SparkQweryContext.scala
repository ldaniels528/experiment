package com.qwery.platform.spark

import com.qwery.models._
import com.qwery.platform.QweryContext
import com.qwery.platform.spark.SparkQweryCompiler.read
import com.qwery.util.OptionHelper._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.util.Properties

/**
  * Qwery Context for Spark
  * @author lawrence.daniels@gmail.com
  */
class SparkQweryContext() extends QweryContext {
  private val logger = LoggerFactory.getLogger(getClass)
  private val procedures = TrieMap[String, SparkProcedure]()
  private val tables = TrieMap[String, TableLike]()
  private val variables = TrieMap[String, Option[DataFrame]]()

  // the optional main program configuration
  var mainProgram: Option[SparkMain] = None

  // start the Spark context
  lazy val spark: SparkSession = createSparkSession(mainProgram)

  /**
    * Registers the given table or view to this context
    * @param procedure the given [[SparkProcedure procedure]]
    */
  def +=(procedure: SparkProcedure): Unit = procedures(procedure.name) = procedure

  /**
    * Registers the given table or view to this context
    * @param tableOrView the given [[TableLike table or view]]
    */
  def +=(tableOrView: TableLike): Unit = tables(tableOrView.name) = tableOrView

  /**
    * Attempts to retrieve the desired procedure by name
    * @param name the name of the procedure
    * @return the [[SparkProcedure]]
    */
  def getProcedure(name: String): SparkProcedure = procedures.get(name) orFail s"Procedure '$name' is not registered"

  /**
    * Attempts to retrieve the desired table by name
    * @param name the name of the table or view
    * @return the [[DataFrame]]
    */
  def getQuery(name: String, alias: Option[String]): Option[DataFrame] = variables.getOrElseUpdate(name, {
    val df = read(getTableOrView(name))(this)
    df.foreach(_.createOrReplaceTempView(name))
    df
  })

  /**
    * Attempts to retrieve the desired table by name
    * @param name the given table name
    * @return the [[Table table]]
    */
  def getTableOrView(name: String): TableLike = tables.get(name) orFail s"Table '$name' is not registered"

  /**
    * Attempts to retrieve the desired table by name
    * @param location the given [[Location]]
    * @return the [[Table table]]
    */
  def getTableOrView(location: Location): TableLike = location match {
    case LocationRef(path) => throw new IllegalStateException("Writing to locations is not yet supported")
    case ref@TableRef(name) => tables.get(name) orFail s"${location.description} is not registered" // TODO alias???
  }

  def toDataFrame(columns: Seq[Column], source: SparkInvokable): Option[DataFrame] =
    source.execute(input = None)(this).map(_.toDF(columns.map(_.name): _*))

  /**
    * Updates the state of a variable by name
    * @param name  the name of the variable to update
    * @param value the updated value
    */
  def updateVariable(name: String, value: Option[DataFrame]): Unit = variables(name) = value

  /**
    * Creates a new Spark session
    * @param mainProgram the optional [[SparkMain]]
    * @return the [[SparkSession]]
    */
  private def createSparkSession(mainProgram: Option[SparkMain]): SparkSession = {
    mainProgram.foreach(app => logger.info(s"Starting Application '${app.name}'..."))
    val builder = SparkSession.builder()
      .master("local[*]") // TODO use environment variable to override this
      .appName(mainProgram.map(_.name) || "Untitled")
      .config("spark.sql.warehouse.dir", Properties.tmpDir)

    // set the optional stuff
    mainProgram foreach { app =>
      if (app.hiveSupport) builder.enableHiveSupport()
    }
    builder.getOrCreate()
  }

}

