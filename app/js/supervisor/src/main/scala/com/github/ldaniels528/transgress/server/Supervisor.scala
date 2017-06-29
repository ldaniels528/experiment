package com.github.ldaniels528.transgress
package server

import com.github.ldaniels528.transgress.AppConstants._
import com.github.ldaniels528.transgress.EnvironmentHelper._
import com.github.ldaniels528.transgress.StringHelper._
import com.github.ldaniels528.transgress.server.routes._
import io.scalajs.nodejs.process
import io.scalajs.npm.bodyparser.{BodyParser, UrlEncodedBodyOptions}
import io.scalajs.npm.express.fileupload.ExpressFileUpload
import io.scalajs.npm.express.{Application, Express, Request, Response}
import io.scalajs.npm.expressws._
import io.scalajs.npm.mongodb.{Db, MongoClient}

import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}

/**
  * Transgress Supervisor
  * @author lawrence.daniels@gmail.com
  */
object Supervisor extends js.JSApp {
  private val logger = LoggerFactory.getLogger(getClass)

  @JSExport
  override def main(): Unit = {
    logger.info(f"Starting the Bourne Server v$Version%.1f...")

    // determine the port to listen on
    val startTime = js.Date.now()

    // setup mongodb connection
    logger.info("Loading MongoDB module...")
    val dbConnect = process.dbConnectOrDefault
    logger.info("Connecting to database '%s'...", dbConnect)
    MongoClient.connectFuture(dbConnect) onComplete {
      case Success(db) =>
        implicit val _db = db

        // setup the application
        val port = process.port getOrElse "9000"
        val app = configureApplication()
        app.listen(port, () => logger.info("Server now listening on port %s [%d msec]", port, js.Date.now() - startTime))

        // handle any uncaught exceptions
        process.onUncaughtException { err =>
          logger.error("An uncaught exception was fired:")
          logger.error(err.stack)
        }

      case Failure(e) =>
        logger.error(s"Error connecting to database: ${e.getMessage}")
    }
  }

  def configureApplication()(implicit db: Db): Application with WsRouting = {
    logger.info("Loading Express modules...")
    implicit val app = Express().withWsRouting
    implicit val wss = ExpressWS(app)

    // setup the routes for serving static files
    logger.info("Setting up the routes for serving static files...")
    app.use(ExpressFileUpload())
    app.use(Express.static("public"))
    app.use("/bower_components", Express.static("bower_components"))

    // setup the body parsers
    logger.info("Loading Body Parser...")
    app.use(BodyParser.json())
      .use(BodyParser.urlencoded(new UrlEncodedBodyOptions(extended = true)))

    // setup stylus & nib for CSS3
    /*
    logger.info("Loading Stylus and Nib modules...")
    app.use(Stylus.middleware(new MiddlewareOptions(src = "public", compile = (str: String, file: String) => {
      Stylus(str)
        .set("filename", file)
        .use(Nib())
    })))*/

    // disable caching
    app.disable("etag")

    // setup logging of the request - response cycles
    app.use((request: Request, response: Response, next: NextFunction) => {
      val startTime = js.Date.now()
      next()
      response.onFinish(() => {
        val elapsedTime = js.Date.now() - startTime
        val query = if (request.query.nonEmpty) (request.query map { case (k, v) => s"$k=$v" } mkString ",").limitTo(120) else "..."
        logger.info("[node] application - %s %s (%s) ~> %d [%d ms]", request.method, request.path, query, response.statusCode, elapsedTime)
      })
    })

    // setup web socket routes
    logger.info("Setting up web socket...")
    app.ws("/websocket", callback = (ws: WebSocket, request: Request) => {
      ws.onMessage(WebSocketHandler.messageHandler(ws, request, _))
    })

    // setup all other routes
    logger.info("Setting up all other routes...")
    new FeedRoutes(app, db)
    new JobRoutes(app, db)
    new SlaveRoutes(app, db)
    new WorkflowRoutes(app, db)
    app
  }

}
