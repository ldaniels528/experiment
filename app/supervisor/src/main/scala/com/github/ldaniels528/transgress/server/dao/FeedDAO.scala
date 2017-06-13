package com.github.ldaniels528.transgress.server.dao

import com.github.ldaniels528.transgress.LoggerFactory
import com.github.ldaniels528.transgress.models.FeedLike
import io.scalajs.JSON
import io.scalajs.npm.mongodb._

import scala.concurrent.{ExecutionContext, Future}
import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Feed Collection DAO
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait FeedDAO extends GenericDAO[FeedData]

/**
  * Represents a Feed Collection
  * @author lawrence.daniels@gmail.com
  */
object FeedDAO {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Feed DAO enrichment
    * @param dao the given [[FeedDAO data access object]]
    */
  final implicit class FeedDAOEnrichment(val dao: FeedDAO) extends AnyVal {

    @inline
    def upsertFeed(data: FeedData)(implicit ec: ExecutionContext): Future[FeedData] = {
      dao.findOneAndUpdate(
        filter = doc("filename" $eq data.filename),
        update = doc(
          $setOnInsert("createdTime" -> data.createdTime),
          $set("mtime" -> data.mtime, "size" -> data.size)
        ),
        options = new FindAndUpdateOptions(upsert = true, returnOriginal = true)).toFuture flatMap {
        case result if result.value != null => Future.successful(result.value.asInstanceOf[FeedData])
        case result if result.isOk => Future.successful(data)
        case result =>
          logger.error(s"result => ${JSON.stringify(result)}")
          Future.failed(js.JavaScriptException(s"ERROR: Failed to create or update feed '${data.filename}'"))
      }
    }

  }

  /**
    * Feed DAO Constructor
    * @param db the given [[Db database]] instance
    */
  final implicit class FeedDAOConstructor(val db: Db) extends AnyVal {

    @inline
    def getFeedDAO: FeedDAO = db.collection("feeds").asInstanceOf[FeedDAO]

  }

}

/**
  * Represents a feed document
  * @param _id      the MongoDB identifier
  * @param filename the feed filename
  * @param mtime    the feed last modified time
  * @param size     the feed size
  */
@ScalaJSDefined
class FeedData(val _id: js.UndefOr[ObjectID] = js.undefined,
               val filename: js.UndefOr[String],
               val mtime: js.UndefOr[Double],
               val size: js.UndefOr[Double],
               val createdTime: js.UndefOr[js.Date] = js.undefined)
  extends FeedLike