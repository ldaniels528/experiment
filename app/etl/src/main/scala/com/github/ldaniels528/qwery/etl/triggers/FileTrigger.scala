package com.github.ldaniels528.qwery.etl.triggers

import java.io.File

import com.github.ldaniels528.qwery.etl.{ETLConfig, ScriptSupport}
import com.github.ldaniels528.qwery.ops.{Executable, ResultSet, Scope}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Represents a File Trigger
  * @param name        the name of the trigger
  * @param constraints the given collection of constraints
  * @param executable  the given executable
  */
case class FileTrigger(name: String, constraints: Seq[Constraint], executable: Executable) extends Trigger {

  override def accepts(scope: Scope, path: String): Boolean = constraints.forall(_.matches(path))

  override def execute(scope: Scope, path: String): ResultSet = executable.execute(scope)

}

/**
  * File Trigger
  * @author lawrence.daniels@gmail.com
  */
object FileTrigger {
  private[this] lazy val log = LoggerFactory.getLogger(getClass)

  /**
    * Loads the triggers found in ./config/triggers.json
    */
  def loadTriggers(config: ETLConfig, configDir: File): List[FileTrigger] = {
    import net.liftweb.json
    implicit val defaults = json.DefaultFormats

    val triggersFile = new File(configDir, "triggers.json")
    if (triggersFile.exists()) {
      log.info(s"Loading triggers from '${triggersFile.getCanonicalPath}'...")
      val triggersJs = json.parse(Source.fromFile(triggersFile).mkString).extract[List[TriggerRaw]]
      triggersJs.map(_.toModel(config))
    }
    else Nil
  }

  /**
    * Represents a trigger JSON object
    * @param name        the name of the trigger
    * @param constraints the given [[ConstraintRaw constraints]]
    * @param script      the given script to execute when triggered
    */
  case class TriggerRaw(name: String, constraints: Seq[ConstraintRaw], script: String) extends ScriptSupport {

    def toModel(config: ETLConfig) = FileTrigger(name, constraints.flatMap(_.toModel), compileScript(config, name, script))

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