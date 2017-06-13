package com.github.ldaniels528.transgress.server
package dao

import io.scalajs.npm.mongodb._

import scala.concurrent.{ExecutionContext, Future}
import scala.scalajs.js

/**
  * Represents a Generic DAO
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait GenericDAO[+T] extends Collection

/**
  * Generic DAO Companion
  * @author lawrence.daniels@gmail.com
  */
object GenericDAO {

  /**
    * Generic DAO enrichment
    * @param dao the given [[GenericDAO data access object]]
    */
  final implicit class GenericDAOEnrichment[T <: js.Any](val dao: GenericDAO[T]) extends AnyVal {

    @inline
    def findAll()(implicit ec: ExecutionContext): Future[js.Array[T]] = dao.find[T]().toArray().toFuture

    @inline
    def findOneByID(id: String): Future[Option[T]] = dao.findOneFuture[T](doc("_id" $eq new ObjectID(id)))

  }

}