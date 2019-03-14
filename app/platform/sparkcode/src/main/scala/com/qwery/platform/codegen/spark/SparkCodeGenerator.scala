package com.qwery.platform.codegen.spark

import java.io.{File, PrintWriter}

import com.databricks.spark.avro._
import com.qwery.language.SQLLanguageParser
import com.qwery.models.StorageFormats._
import com.qwery.models._
import com.qwery.models.expressions._
import com.qwery.platform.codegen.spark.SparkCodeGenerator.MainClass
import com.qwery.platform.codegen.spark.SparkCodeGenerator.implicits._
import com.qwery.platform.spark.{SparkQweryCompiler, die}
import com.qwery.util.OptionHelper._
import com.qwery.util.ResourceHelper._
import com.qwery.util.StringHelper._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
  * Spark Code Generator
  * @author lawrence.daniels@gmail.com
  */
class SparkCodeGenerator(className: String, packageName: String) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val tables = TrieMap[String, TableLike]()

  def generate(invokable: Invokable): File = {
    val classDefinition =
      MainClass(className, packageName, invokable, imports = List(
        "import org.apache.spark.sql.types.StructType",
        "import org.apache.spark.sql.DataFrame",
        "import org.apache.spark.sql.Row",
        "import org.apache.spark.sql.SparkSession",
        "import org.slf4j.LoggerFactory"
      )).generate

    // write the class to disk
    val outputFile = new File("./temp", s"$className.scala")
    new PrintWriter(outputFile).use(_.println(classDefinition))
    outputFile
  }

  /**
    * Returns the equivalent query operation to represent the given table or view
    * @param tableOrView the given [[TableLike table or view]]
    * @return the [[DataFrame]]
    */
  def read(tableOrView: TableLike)(implicit spark: SparkSession): DataFrame = {
    import SparkQweryCompiler.Implicits._
    tableOrView match {
      case table: Table =>
        val reader = spark.read.tableOptions(table)
        table.inputFormat.orFail("Table input format was not specified") match {
          case AVRO => reader.avro(table.location)
          case CSV => reader.schema(createSchema(table.columns)).csv(table.location)
          case JDBC => reader.jdbc(table.location, table.name, table.properties || new java.util.Properties())
          case JSON => reader.json(table.location)
          case PARQUET => reader.parquet(table.location)
          case ORC => reader.orc(table.location)
          case format => die(s"Storage format $format is not supported for reading")
        }
      case unknown => die(s"Unrecognized table type '$unknown' (${unknown.getClass.getName})")
    }
  }

  def createSchema(columns: Seq[Column]): StructType = {
    import SparkQweryCompiler.Implicits._
    StructType(fields = columns.map(_.compile))
  }

}

/**
  * Spark Code Generator Companion
  * @author lawrence.daniels@gmail.com
  */
object SparkCodeGenerator {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * Creates a new Spark Code Generator
    * @param classNameWithPackage the given class and package names (e.g. "com.acme.spark.MyFirstSparkJob")
    * @return a [[SparkCodeGenerator]]
    * @example {{{ java com.qwery.platform.codegen.spark.SparkCodeGenerator ./samples/sql/companylist.sql com.acme.spark.MyFirstSparkJob }}}
    */
  def apply(classNameWithPackage: String): SparkCodeGenerator = {
    classNameWithPackage.lastIndexOfOpt(".").map(classNameWithPackage.splitAt) match {
      case Some((className, packageName)) => new SparkCodeGenerator(className, packageName)
      case None => new SparkCodeGenerator(classNameWithPackage, packageName = "com.qwery.examples")
    }
  }

  /**
    * For stand alone operation
    * @param args the given command line arguments
    */
  def main(args: Array[String]): Unit = {
    // check the command line arguments
    args.toList match {
      case sqlFile :: className :: genArgs =>
        val sql = SQLLanguageParser.parse(new File(sqlFile))
        SparkCodeGenerator(className).generate(sql)
      case _ =>
        die(s"java ${getClass.getName.replaceAllLiterally("$", "")} <scriptFile> <outputClass> [<arg1> .. <argN>]")
    }
  }

  case class MainClass(className: String,
                       packageName: String,
                       invokable: Invokable,
                       imports: Seq[String]) extends InjectedCode {
    override def generate: String = {
      s"""|package $packageName
          |
          |${imports.sortBy(s => s).mkString("\n")}
          |
          |class $className() extends Serializable {
          |  @transient private val logger = LoggerFactory.getLogger(getClass)
          |
          |  def start(args: Array[String])(implicit spark: SparkSession): Unit = {
          |     ${invokable.decode}
          |  }
          |}
          |
          |object $className {
          |   private[this] val logger = LoggerFactory.getLogger(getClass)
          |
          |   def main(args: Array[String]): Unit = {
          |     implicit val spark: SparkSession = createSparkSession("$className")
          |     new $className().start(args)
          |     spark.stop()
          |   }
          |
          |   def createSparkSession(appName: String): SparkSession = {
          |     val sparkConf = new SparkConf()
          |     val builder = SparkSession.builder()
          |       .appName(appName)
          |       .config(sparkConf)
          |       .enableHiveSupport()
          |
          |     // first attempt to create a clustered session
          |     try builder.getOrCreate() catch {
          |       // on failure, create a local one...
          |       case _: Throwable =>
          |         logger.warn(s"$$appName failed to connect to EMR cluster; starting local session...")
          |         builder.master("local[*]").getOrCreate()
          |     }
          |   }
          |}
          |""".stripMargin
    }
  }

  /**
    * implicits classes & conversions
    */
  object implicits {

    final implicit class Decoder(val invokable: Invokable) extends AnyVal {

      def decode: String = {
        logger.info(s"Decoding '$invokable'...")
        invokable match {
          case i: Insert => decodeInsert(i)
          case m: MainProgram => m.code.decode
          case s: SQL => decodeSQL(s)
          case s: Select => decodeSelect(s)
          case x =>
            throw new IllegalArgumentException(s"Unsupported operation ${Option(x).map(_.getClass.getName).orNull}")
        }
      }

      private def decodeInsert(insert: Insert): String = {
        """|
           |
           |""".stripMargin
      }

      private def decodeSelect(select: Select): String = {
        import select._
        s"""|SELECT ${fields.mkString(",")}
            | ${from.map(table => s"FROM $table") || ""}
            | ${where.map(condition => s"WHERE $condition") || ""}
            |""".stripMargin
      }

      private def decodeSQL(sql: SQL): String = {
        s"""|{
            |  ${sql.statements.map(stmt => stmt.decode).mkString("\n")}
            |}
            |""".stripMargin
      }

      private def decodeSetLocalVar(op: SetLocalVariable) = {

      }

    }

  }

}
