package com.github.ldaniels528.transgress.server
package dao

import com.github.ldaniels528.transgress.models.SlaveLike
import io.scalajs.npm.mongodb._

import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Slave DAO
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait SlaveDAO extends GenericDAO[SlaveData]

/**
  * Slave DAO Singleton
  * @author lawrence.daniels@gmail.com
  */
object SlaveDAO {

  /**
    * Slave DAO enrichment
    * @param dao the given [[SlaveDAO data access object]]
    */
  final implicit class SlaveDAOEnrichment(val dao: SlaveDAO) extends AnyVal {

    @inline
    def findOneByHost(host: String): Future[Option[SlaveData]] = {
      dao.findOneFuture[SlaveData](doc("host" $eq host))
    }

    @inline
    def upsertSlave(data: SlaveData): js.Promise[FindAndModifyWriteOpResult] = {
      dao.findOneAndUpdate(
        filter = doc("host" -> data.host, "port" -> data.port),
        update = doc(
          $set(
            "concurrency" -> data.concurrency,
            "lastUpdated" -> new js.Date(),
            "maxConcurrency" -> data.maxConcurrency
          )),
        options = new FindAndUpdateOptions(upsert = true, returnOriginal = false)
      )
    }

    @inline
    def updateSlave(data: SlaveData): js.Promise[FindAndModifyWriteOpResult] = {
      dao.findOneAndUpdate(
        filter = doc("host" -> data.host, "port" -> data.port),
        update = doc(
          $set(
            "name" -> data.name,
            "concurrency" -> data.concurrency,
            "lastUpdated" -> new js.Date(),
            "maxConcurrency" -> data.maxConcurrency
          )),
        options = new FindAndUpdateOptions(upsert = true, returnOriginal = false)
      )
    }

  }

  /**
    * Slave DAO Constructor
    * @param db the given [[Db database]] instance
    */
  final implicit class SlaveDAOConstructor(val db: Db) extends AnyVal {

    @inline
    def getSlaveDAO: SlaveDAO = db.collection("slaves").asInstanceOf[SlaveDAO]

  }

}

/**
  * Represents a Slave document
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class SlaveData(var _id: js.UndefOr[ObjectID] = js.undefined,
                var name: js.UndefOr[String] = js.undefined,
                var host: js.UndefOr[String],
                var port: js.UndefOr[String],
                var maxConcurrency: js.UndefOr[Int],
                var concurrency: js.UndefOr[Int] = 0,
                var lastUpdated: js.UndefOr[js.Date] = js.undefined)
  extends SlaveLike