package com.qwery.platform.flink

import com.qwery.models._
import com.qwery.platform.QweryContext
import com.qwery.util.OptionHelper._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

import scala.collection.concurrent.TrieMap

/**
  * Qwery Runtime Context for Flink
  * @author lawrence.daniels@gmail.com
  */
class FlinkQweryContext() extends QweryContext {
  private val procedures = TrieMap[String, FlinkProcedure]()
  private val tables = TrieMap[String, TableLike]()
  private val variables = TrieMap[String, Option[DataFrame]]()

  // the optional main program configuration
  var mainProgram: Option[FlinkMain] = None

  // setup the streaming environment
  lazy val env: StreamExecutionEnvironment = {
    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    see
  }

  // setup the table environment
  lazy val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

  /**
    * Registers the given table or view to this context
    * @param procedure the given [[FlinkProcedure procedure]]
    */
  def +=(procedure: FlinkProcedure): Unit = procedures(procedure.name) = procedure

  /**
    * Registers the given table or view to this context
    * @param tableOrView the given [[TableLike table or view]]
    */
  def +=(tableOrView: TableLike): Unit = tables(tableOrView.name) = tableOrView

  /**
    * Starts the streaming event processing
    */
  def start(): Unit = env.execute(jobName = mainProgram.map(_.name) || "Untitled")

  /**
    * Attempts to retrieve the desired table by name
    * @param name the name of the table or view
    * @return the [[DataFrame]]
    */
  def getQuery(name: String): Option[DataFrame] = variables.getOrElseUpdate(name, FlinkQweryCompiler.read(getTableOrView(name))(this))

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
    case LocationRef(path) => die("Writing to locations is not yet supported")
    case ref@TableRef(name) => tables.get(name) orFail s"${location.description} is not registered" // TODO alias???
  }

  /**
    * Updates the state of a variable by name
    * @param name  the name of the variable to update
    * @param value the updated value
    */
  def updateVariable(name: String, value: Option[DataFrame]): Unit = variables(name) = value

}
