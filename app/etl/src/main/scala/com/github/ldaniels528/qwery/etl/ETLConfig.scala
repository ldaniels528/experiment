package com.github.ldaniels528.qwery
package etl

import java.io.File

import akka.actor.ActorRef
import com.github.ldaniels528.qwery.actors.QweryActorSystem
import com.github.ldaniels528.qwery.etl.ETLConfig.TriggerRaw
import com.github.ldaniels528.qwery.etl.actors.{FileManagementActor, WorkflowManagementActor}
import com.github.ldaniels528.qwery.etl.triggers._
import com.github.ldaniels528.qwery.ops.{Executable, Scope}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.io.Source

/**
  * ETL Configuration
  * @author lawrence.daniels@gmail.com
  */
class ETLConfig(val baseDir: File) {
  private lazy val log = LoggerFactory.getLogger(getClass)
  private val triggers = TrieMap[String, Trigger]()

  // define the processing directories
  val archiveDir: File = new File(baseDir, "archive")
  val configDir: File = new File(baseDir, "config")
  val failedDir: File = new File(baseDir, "failed")
  val inboxDir: File = new File(baseDir, "inbox")
  val scriptsDir: File = new File(baseDir, "scripts")
  val workDir: File = new File(baseDir, "work")

  // create the support actors
  val fileManager: ActorRef = QweryActorSystem.createActor(name = "fm", () => new FileManagementActor(this))
  val workflowManager: ActorRef = QweryActorSystem.createActor(name = "wm", () => new WorkflowManagementActor(this))

  // installation checks
  ensureSubdirectories(baseDir)

  /**
    * Adds the given trigger to the configuration
    * @param trigger the given [[Trigger trigger]]
    */
  def add(trigger: Trigger): Unit = triggers(trigger.name) = trigger

  /**
    * ensures the existence of the processing sub-directories
    * @param baseDir the given base directory
    */
  private def ensureSubdirectories(baseDir: File) = {
    // make sure it exists
    if (!baseDir.exists || !baseDir.isDirectory)
      throw new IllegalStateException(s"Qwery Home directory '${baseDir.getAbsolutePath}' does not exist")

    val subDirectories = Seq(archiveDir, configDir, failedDir, inboxDir, scriptsDir, workDir)
    subDirectories.foreach { directory =>
      if (!directory.exists()) {
        log.info(s"Creating directory '${directory.getAbsolutePath}'...")
        directory.mkdir()
      }
    }
  }

  /**
    * Attempts to find a trigger that accepts the given file
    * @param scope the given [[Scope scope]]
    * @param file  the given file
    * @return an option of a [[Trigger]]
    */
  def lookupTrigger(scope: Scope, file: String): Option[Trigger] = triggers.find(_._2.accepts(scope, file)).map(_._2)

  /**
    * Loads the triggers found in ./config/triggers.json
    */
  def loadTriggers(): Unit = {
    import net.liftweb.json
    implicit val defaults = json.DefaultFormats

    val triggersFile = new File(configDir, "triggers.json")
    if (triggersFile.exists()) {
      log.info(s"Loading triggers from '${triggersFile.getCanonicalPath}'...")
      val triggersJs = json.parse(Source.fromFile(triggersFile).mkString).extract[List[TriggerRaw]]
      triggersJs.map(_.toModel(this)) foreach this.add
    }
  }

}

/**
  * ETLConfig Companion
  * @author lawrence.daniels@gmail.com
  */
object ETLConfig {
  private[this] lazy val log = LoggerFactory.getLogger(getClass)

  /**
    * Represents a trigger JSON object
    * @param name        the name of the trigger
    * @param constraints the given [[ConstraintRaw constraints]]
    * @param script      the given script to execute when triggered
    */
  case class TriggerRaw(name: String, constraints: Seq[ConstraintRaw], script: String) {

    def toModel(config: ETLConfig) = FileTrigger(name, constraints.flatMap(_.toModel), compileScript(config))

    private def compileScript(config: ETLConfig): Executable = {
      val scriptFile = new File(config.scriptsDir, script)
      log.info(s"[$name] Compiling script '${scriptFile.getName}'...")
      QweryCompiler(Source.fromFile(scriptFile).mkString)
    }
  }

  /**
    * Represents a constraint JSON object
    * @param contains   represents a "contains" constraint (e.g. "constraints": [{"prefix": "companylist"}])
    * @param equals     represents a "equals" constraint (e.g. "constraints": [{"equals": "companylist.csv"}])
    * @param prefix     represents a "prefix" constraint (e.g. "constraints": [{"prefix": "company"}])
    * @param regex      represents a "regex" constraint (e.g. "constraints": [{"regex": "company*.csv"}])
    * @param suffix     represents a "suffix" constraint (e.g. "constraints": [{"suffix": ".csv"}])
    * @param ignoreCase represents a "ignoreCase" constraint (e.g. "constraints": [{"ignoreCase": true}])
    */
  case class ConstraintRaw(contains: Option[String],
                           equals: Option[String],
                           prefix: Option[String],
                           regex: Option[String],
                           suffix: Option[String],
                           ignoreCase: Option[Boolean]) {
    def toModel: Seq[Constraint] = {
      val ignoresCase = ignoreCase.contains(true)
      contains.map(contains => ContainsConstraint(contains, ignoreCase = ignoresCase)).toList :::
        equals.map(equals => EqualsConstraint(equals, ignoreCase = ignoresCase)).toList :::
        prefix.map(prefix => PrefixConstraint(prefix, ignoreCase = ignoresCase)).toList :::
        regex.map(regex => RegExConstraint(regex)).toList :::
        suffix.map(suffix => SuffixConstraint(suffix, ignoreCase = ignoresCase)).toList
    }
  }

}