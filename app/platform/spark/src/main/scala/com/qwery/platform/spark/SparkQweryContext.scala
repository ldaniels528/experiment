package com.qwery.platform.spark

import com.qwery.models._
import com.qwery.platform.spark.SparkQweryCompiler.read
import com.qwery.platform.{QweryContext, Scope}
import com.qwery.util.OptionHelper._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.util.Properties

/**
  * Qwery Context for Spark
  * @author lawrence.daniels@gmail.com
  */
class SparkQweryContext() extends QweryContext {
  private val logger = LoggerFactory.getLogger(getClass)

  // the optional main program configuration
  var mainProgram: Option[SparkMain] = None

  // start the Spark context
  lazy val spark: SparkSession = createSparkSession(mainProgram)

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

  ////////////////////////////////////////////////////////////////////////
  //      DataSet methods
  ////////////////////////////////////////////////////////////////////////
  private val dataSets = TrieMap[String, Option[DataFrame]]()

  /**
    * Creates a data set from the given source
    * @param columns the column definitions
    * @param source  the given [[SparkInvokable source]]
    * @return the option of a [[DataFrame]]
    */
  def createDataSet(columns: Seq[Column], source: SparkInvokable): Option[DataFrame] =
    source.execute(input = None)(this).map(_.toDF(columns.map(_.name): _*))

  /**
    * Creates a data set from the given source
    * @param columns the column definitions
    * @param data    the given collection of rows
    * @return the option of a [[DataFrame]]
    */
  def createDataSet(columns: Seq[Column], data: Seq[Seq[Any]]): DataFrame = {
    val rows = data.map(values => Row(values: _*))
    val rdd = spark.sparkContext.makeRDD(rows)
    spark.sqlContext.createDataFrame(rdd, createSchema(columns))
  }

  /**
    * Creates a schema
    * @param columns the given [[Column]] definitions
    * @return the [[StructType]]
    */
  def createSchema(columns: Seq[Column]): StructType = {
    import SparkQweryCompiler.Implicits._
    StructType(fields = columns.map(_.compile))
  }

  /**
    * Attempts to retrieve the desired table by name
    * @param name the name of the table or view
    * @return the [[DataFrame]]
    */
  def getDataSet(name: String, alias: Option[String]): Option[DataFrame] = dataSets.getOrElseUpdate(name, {
    val df = read(getTableOrView(name))(this)
    df.foreach(_.createOrReplaceTempView(name))
    df
  })

  /**
    * Updates the state of a data set variable by name
    * @param name  the name of the variable to update
    * @param value the updated value
    */
  def updateDataSet(name: String, value: Option[DataFrame]): Unit = dataSets(name) = value

  ////////////////////////////////////////////////////////////////////////
  //      Procedure methods
  ////////////////////////////////////////////////////////////////////////
  private val procedures = TrieMap[String, SparkProcedure]()

  /**
    * Registers the given table or view to this context
    * @param procedure the given [[SparkProcedure procedure]]
    */
  def +=(procedure: SparkProcedure): Unit = procedures(procedure.name) = procedure

  /**
    * Attempts to retrieve the desired procedure by name
    * @param name the name of the procedure
    * @return the [[SparkProcedure]]
    */
  def getProcedure(name: String): SparkProcedure = procedures.get(name) orFail s"Procedure '$name' is not registered"

  ////////////////////////////////////////////////////////////////////////
  //      Scope methods
  ////////////////////////////////////////////////////////////////////////
  private var scopes: List[Scope] = Nil

  /**
    * Attempts to retrieve the desired variable by name
    * @param name the given variable name
    * @return the value
    */
  def getVariable(name: String): Any = scopes.headOption.flatMap(_.apply(name)) orFail s"Variable '$name' was not found"

  /**
    * Creates a scope to be passed into the given executable block; then destroys the scope upon completion.
    * @param block the given executable block
    * @tparam A the parameterized return type
    * @return the result of the executable block
    */
  def withScope[A](block: Scope => A): A = {
    // create a new scope
    val scope = Scope()
    scopes = scope :: scopes

    // execute the block
    val result = block(scope)

    // destroy the scope
    scopes = scopes.tail

    // return the result
    result
  }

  ////////////////////////////////////////////////////////////////////////
  //      Table-Like methods
  ////////////////////////////////////////////////////////////////////////
  private val tables = TrieMap[String, TableLike]()

  /**
    * Registers the given table or view to this context
    * @param tableOrView the given [[TableLike table or view]]
    */
  def +=(tableOrView: TableLike): Unit = tables(tableOrView.name) = tableOrView

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
    case ref@TableRef(name) => tables.get(name) ?? ref.alias.flatMap(tables.get) orFail s"${location.description} is not registered"
  }

}

