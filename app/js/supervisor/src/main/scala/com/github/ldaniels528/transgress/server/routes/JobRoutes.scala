package com.github.ldaniels528.transgress.server
package routes

import com.github.ldaniels528.transgress.LoggerFactory
import com.github.ldaniels528.transgress.RemoteEvent._
import com.github.ldaniels528.transgress.models.{JobLike, JobStates, StatisticsLike, StatusMessage}
import com.github.ldaniels528.transgress.server.dao.JobDAO._
import com.github.ldaniels528.transgress.server.dao.JobData
import com.github.ldaniels528.transgress.server.dao.SlaveDAO._
import io.scalajs.JSON
import io.scalajs.npm.express.{Application, Request, Response}
import io.scalajs.npm.expressws.WsRouting
import io.scalajs.npm.mongodb._
import io.scalajs.npm.request.{Request => Client}
import io.scalajs.util.JsUnderOrHelper._

import scala.concurrent.{ExecutionContext, Future}
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.{Failure, Success}

/**
  * Job Routes
  * @author lawrence.daniels@gmail.com
  */
class JobRoutes(app: Application with WsRouting, db: Db)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val jobDAO = db.getJobDAO
  private val slaveDAO = db.getSlaveDAO

  /////////////////////////////////////////////////////////
  //    Accessor Routes
  /////////////////////////////////////////////////////////

  /**
    * Retrieves a job by ID
    */
  app.get("/api/job/:id", (request: Request, response: Response, next: NextFunction) => {
    val id = request.params.apply("id")
    jobDAO.findOneByID(id) onComplete {
      case Success(Some(job)) => response.send(js.Array(job)); next()
      case Success(None) => response.send(js.Array()); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

  /**
    * Retrieve jobs by state
    */
  app.get("/api/jobs", (request: Request, response: Response, next: NextFunction) => {
    val states = request.query.get("states") match {
      case Some(state) => state.split("[|]").toJSArray
      case None => JobStates.values.toJSArray
    }
    jobDAO.findByState(states: _*).toArray().toFuture onComplete {
      case Success(jobs) => response.send(jobs); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

  /////////////////////////////////////////////////////////
  //    Mutator Routes for Specific Jobs
  /////////////////////////////////////////////////////////

  /**
    * Change a job's state
    */
  app.patch("/api/job/:id/state/:state", (request: Request, response: Response, next: NextFunction) => {
    val (id, state) = (request.params.apply("id"), request.params.apply("state"))
    val message = request.bodyAs[StatusMessage].message.flat
    jobDAO.changeState(id, state, message).toFuture onComplete {
      case Success(result) if result.isOk && result.value != null =>
        WebSocketHandler.emit(JOB_UPDATE, result.value)
        response.send(js.Array(result.value))
        next()
      case Success(result) => response.notFound(s"Job state not updated: ${JSON.stringify(result)}"); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

  /**
    * Updates job statistics
    */
  app.patch("/api/job/:id/statistics", (request: Request, response: Response, next: NextFunction) => {
    val jobId = request.params.apply("id")
    val statistics = request.bodyAs[StatisticsLike]
    jobDAO.updateStatistics(jobId, statistics).toFuture onComplete {
      case Success(result) if result.value != null =>
        WebSocketHandler.emit(JOB_UPDATE, result.value)
        response.send(result.value)
        next()
      case Success(result) => response.notFound(s"Job statistics not updated: ${JSON.stringify(result)}"); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

  /**
    * Pauses a job
    */
  app.get("/api/job/:jobID/pause/:slaveID", (request: Request, response: Response, next: NextFunction) => {
    jobManagement(request, response, action = "pause") onComplete {
      case Success(job) => response.send(job); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

  /**
    * Resumes a job
    */
  app.get("/api/job/:jobID/resume/:slaveID", (request: Request, response: Response, next: NextFunction) => {
    jobManagement(request, response, action = "resume") onComplete {
      case Success(job) => response.send(job); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

  /**
    * Stops a job
    */
  app.get("/api/job/:jobID/stop/:slaveID", (request: Request, response: Response, next: NextFunction) => {
    jobManagement(request, response, action = "stop") onComplete {
      case Success(job) => response.send(job); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

  private def jobManagement(request: Request, response: Response, action: String) = {
    val jobID = request.params.apply("jobID")
    val slaveID = request.params.apply("slaveID")
    for {
      endpoint_? <- slaveDAO.findOneByID(slaveID) flatMap  {
        case Some(slave) => Future.successful(slave.getEndpoint.toOption)
        case None => Future.failed(js.JavaScriptException(s"Slave $slaveID not found"))
      }
      job <- endpoint_? match {
        case Some(endpoint) => workerRequest(endpoint, s"job/$jobID/$action")
        case None => Future.failed(js.JavaScriptException(s"Job $jobID not found"))
      }
      state = job.state.orDie(s"Job $jobID has no state")
      _ <- jobDAO.changeState(jobID, state, message = job.message).toFuture
    } yield job
  }

  private def workerRequest(endpoint: String, uri: String) = {
    val url = s"http://$endpoint/api/worker/$uri"
    logger.info(s"WORKER call ~> $url")
    Client.getFuture(url) map {
      case (_, body) =>
        logger.info(s"WORKER RESPONSE ${JSON.stringify(body)}")
        body match {
          case s if js.typeOf(s) == "string" => JSON.parseAs[JobLike](body.asInstanceOf[String])
          case o => o.asInstanceOf[JobLike]
        }
    }
  }

  /////////////////////////////////////////////////////////
  //    Mutator Routes for Collections of Jobs
  /////////////////////////////////////////////////////////

  /**
    * Creates a new job
    */
  app.post("/api/jobs", (request: Request, response: Response, next: NextFunction) => {
    val job = request.bodyAs[JobData]
    jobDAO.createJob(job).toFuture onComplete {
      case Success(result) if result.insertedCount == 1 =>
        result.ops.foreach(WebSocketHandler.emit(JOB_UPDATE, _))
        response.send(result.ops)
        next()
      case Success(result) => response.badRequest(s"Job not created: ${JSON.stringify(result)}")
      case Failure(e) => response.internalServerError(e); next()
    }
  })

  /**
    * Retrieves the next job from the queue
    */
  app.patch("/api/jobs/checkout/:slaveID", (request: Request, response: Response, next: NextFunction) => {
    val slaveID = request.params.apply("slaveID")
    val outcome = for {
      slave_? <- slaveDAO.findOneByID(slaveID)
      endpoint_? = for {slave <- slave_?; host <- slave.host.toOption; port <- slave.port.toOption} yield s"$host:$port"
      result <- endpoint_? match {
        case Some(endpoint) => jobDAO.checkoutJob(slaveID, endpoint).toFuture
        case None => Future.failed(js.JavaScriptException(s"Slave # $slaveID not found"))
      }
    } yield result

    outcome onComplete {
      case Success(result) if result.value != null =>
        WebSocketHandler.emit(JOB_UPDATE, result.value)
        response.send(js.Array(result.value))
        next()
      case Success(_) => response.send(js.Array[js.Any]()); next()
      case Failure(e) => response.internalServerError(e); next()
    }
  })

}
