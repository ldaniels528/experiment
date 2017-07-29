package com.github.ldaniels528.broadway.watcher

import java.io.{File, FileNotFoundException}

import com.github.ldaniels528.broadway.watcher.WatcherConfig.Directory
import com.github.ldaniels528.qwery.util.JSONSupport
import com.github.ldaniels528.qwery.util.OptionHelper._

import scala.io.Source

/**
  * Watcher Configuration
  * @param supervisor the supervisor end point (e.g. "localhost:9000")
  */
case class WatcherConfig(supervisor: String, directories: Seq[Directory])

/**
  * Watcher configuration
  * @author lawrence.daniels@gmail.com
  */
object WatcherConfig extends JSONSupport {

  /**
    * Loads the optional ETL Watcher (Watcher.json) configuration file
    * @param configFile the given [[File configuration file]]
    * @return the [[WatcherConfig processing configuration]]
    */
  def load(configFile: File): WatcherConfig = {
    if (configFile.exists()) {
      val WatcherConfigJs = parseJsonAs[WatcherConfigJSON](Source.fromFile(configFile).mkString)
      WatcherConfigJs.toModel
    }
    else throw new FileNotFoundException(s"Configuration file '${configFile.getAbsolutePath}' not found")
  }

  case class WatcherConfigJSON(supervisor: Option[String], directories: Option[Seq[DirectoryJSON]]) {
    def toModel = WatcherConfig(
      supervisor = supervisor getOrElse "localhost:9000",
      directories = directories.map(_.map(_.toModel)).getOrElse(Nil)
    )
  }

  case class Directory(source: String, target: String, constraints: Seq[ConstraintJSON])

  case class DirectoryJSON(source: Option[String], target: Option[String], constraints: Option[Seq[ConstraintJSON]]) {
    def toModel = Directory(
      source = source.orDie("No source specified"),
      target = target.orDie("No target specified"),
      constraints = constraints.getOrElse(Nil)
    )
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
  case class ConstraintJSON(contains: Option[String],
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