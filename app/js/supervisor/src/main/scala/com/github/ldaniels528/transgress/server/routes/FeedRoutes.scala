package com.github.ldaniels528.transgress.server.routes

import com.github.ldaniels528.transgress.LoggerFactory
import com.github.ldaniels528.transgress.RemoteEvent.FEED_UPDATE
import com.github.ldaniels528.transgress.server.WebSocketHandler
import com.github.ldaniels528.transgress.server.dao.FeedDAO._
import com.github.ldaniels528.transgress.server.dao.FeedData
import io.scalajs.npm.express.{Application, Request, Response}
import io.scalajs.npm.expressws.WsRouting
import io.scalajs.npm.mongodb.Db

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * Feed Routes
  * @author lawrence.daniels@gmail.com
  */
class FeedRoutes(app: Application with WsRouting, db: Db)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val feedDAO = db.getFeedDAO

  /**
    * Creates or updates a feed
    */
  app.post("/api/feed", (request: Request, response: Response, next: NextFunction) => {
    val body = request.bodyAs[FeedData]
    val form = for {
      _ <- body.filename
      _ <- body.size
    } yield body

    form.toOption match {
      case Some(data) =>
        feedDAO.upsertFeed(data) onComplete {
          case Success(feed) =>
            WebSocketHandler.emit(FEED_UPDATE, feed)
            response.send(feed)
            next()
          case Failure(e) =>
            e.printStackTrace()
            response.internalServerError(e)
            next()
        }
      case None =>
        response.badRequest("ERROR: 'filename' and 'fileSize' are required")
    }
  })


}
