package com.github.ldaniels528.transgress.server
package dao

import com.github.ldaniels528.transgress.models.TriggerLike
import io.scalajs.npm.mongodb.{Db, ObjectID}

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Trigger DAO
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait TriggerDAO extends GenericDAO[TriggerData]

/**
  * Trigger DAO Companion
  * @author lawrence.daniels@gmail.com
  */
object TriggerDAO {

  /**
    * Trigger DAO enrichment
    * @param dao the given [[TriggerDAO data access object]]
    */
  final implicit class TriggerDAOEnrichment(val dao: TriggerDAO) extends AnyVal {

  }

  /**
    * Trigger DAO Constructor
    * @param db the given [[Db database]] instance
    */
  final implicit class TriggerDAOConstructor(val db: Db) extends AnyVal {

    @inline
    def getTriggerDAO: TriggerDAO = db.collection("triggers").asInstanceOf[TriggerDAO]

  }

}

/**
  * Represents a trigger document
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class TriggerData(var _id: js.UndefOr[ObjectID] = js.undefined,
                  var name: js.UndefOr[String] = js.undefined,
                  var priority: js.UndefOr[Int] = js.undefined,
                  var patterns: js.UndefOr[js.Array[String]] = js.undefined,
                  var workflowName: js.UndefOr[String] = js.undefined)
  extends TriggerLike