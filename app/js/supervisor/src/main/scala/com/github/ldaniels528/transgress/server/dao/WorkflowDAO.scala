package com.github.ldaniels528.transgress.server
package dao

import com.github.ldaniels528.transgress.models.{OperationLike, SourceLike, VariableLike, WorkflowLike}
import io.scalajs.npm.mongodb._

import scala.concurrent.{ExecutionContext, Future}
import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Workflow DAO
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait WorkflowDAO extends GenericDAO[WorkflowData]

/**
  * Workflow DAO Companion
  * @author lawrence.daniels@gmail.com
  */
object WorkflowDAO {

  /**
    * Workflow DAO enrichment
    * @param dao the given [[WorkflowDAO data access object]]
    */
  final implicit class WorkflowDAOEnrichment(val dao: WorkflowDAO) extends AnyVal {

    @inline
    def findByName(name: String)(implicit ec: ExecutionContext): Future[Option[WorkflowData]] = {
      dao.findOneFuture[WorkflowData](doc("name" $eq name))
    }

  }

  /**
    * Workflow DAO Constructor
    * @param db the given [[Db database]] instance
    */
  final implicit class WorkflowDAOConstructor(val db: Db) extends AnyVal {

    @inline
    def getWorkflowDAO: WorkflowDAO = db.collection("workflows").asInstanceOf[WorkflowDAO]

  }

}

/**
  * Represents a workflow document
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class WorkflowData(var _id: js.UndefOr[ObjectID] = js.undefined,
                   var name: js.UndefOr[String] = js.undefined,
                   var input: js.UndefOr[SourceLike] = js.undefined,
                   var outputs: js.UndefOr[js.Array[SourceLike]] = js.undefined,
                   var events: js.UndefOr[js.Dictionary[OperationLike]] = js.undefined,
                   var variables: js.UndefOr[js.Array[VariableLike]] = js.undefined)
  extends WorkflowLike