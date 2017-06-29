package com.github.ldaniels528.transgress.server
package routes

import com.github.ldaniels528.transgress.LoggerFactory
import com.github.ldaniels528.transgress.RemoteEvent.SLAVE_UPDATE
import com.github.ldaniels528.transgress.server.dao.SlaveDAO._
import com.github.ldaniels528.transgress.server.dao.SlaveData
import io.scalajs.JSON
import io.scalajs.npm.express.{Application, Request, Response}
import io.scalajs.npm.expressws.WsRouting
import io.scalajs.npm.mongodb.Db

import scala.concurrent.ExecutionContext
import scala.scalajs.js
import scala.util.{Failure, Success}

/**
  * Slave Routes
  * @author lawrence.daniels@gmail.com
  */
class SlaveRoutes(app: Application with WsRouting, db: Db)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val slaveDAO = db.getSlaveDAO

  /**
    * Retrieves a slave by name
    */
  app.get("/api/slave", (request: Request, response: Response, next: NextFunction) => {
    request.query.get("name") match {
      case Some(name) =>
        slaveDAO.findOneByHost(name) onComplete {
          case Success(Some(slave)) => WebSocketHandler.emit(SLAVE_UPDATE, slave); response.send(js.Array(slave)); next()
          case Success(None) => response.send(js.Array()); next()
          case Failure(e) => response.internalServerError(e)
        }
      case None =>
        response.badRequest("'name' is required")
    }
  })

  /**
    * Registers a slave
    */
  app.post("/api/slave", (request: Request, response: Response, next: NextFunction) => {
    val body = request.bodyAs[SlaveData]
    val form = for {
      host <- body.host
      port <- body.port if port.matches("\\d+")
    } yield body

    form.toOption match {
      case Some(data) =>
        slaveDAO.upsertSlave(data).toFuture onComplete {
          case Success(wr) if wr.value != null =>
            WebSocketHandler.emit(SLAVE_UPDATE, wr.value)
            response.send(wr.value)
            next()
          case Success(wr) =>
            logger.error(s"POST /api/slave ~> ${JSON.stringify(wr)}")
            response.badRequest("Slave could not be registered")
            next()
          case Failure(e) =>
            e.printStackTrace()
            response.internalServerError(e)
            next()
        }
      case None =>
        response.badRequest("'host' and 'port' are required")
    }
  })

  /**
    * Retrieves active slaves
    */
  app.get("/api/slaves", (request: Request, response: Response, next: NextFunction) => {
    slaveDAO.findAll() onComplete {
      case Success(slaves) => response.send(slaves); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

}
