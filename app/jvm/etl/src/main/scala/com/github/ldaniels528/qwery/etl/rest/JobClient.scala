package com.github.ldaniels528.qwery.etl.rest

import akka.actor.Actor
import com.github.ldaniels528.qwery.etl.actors.JobStates.JobState
import com.github.ldaniels528.qwery.etl.actors.{Job, JobStatistics}
import com.github.ldaniels528.qwery.rest.RESTSupport
import play.api.libs.json.{JsValue, Json}
import play.api.libs.ws.JsonBodyReadables._
import play.api.libs.ws.JsonBodyWritables._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Job REST Client
  * @author lawrence.daniels@gmail.com
  */
trait JobClient extends RESTSupport {
  self: Actor =>

  def baseUrl: String

  def changeState(job: Job, state: JobState)(implicit ec: ExecutionContext): Future[Option[Job]] = {
    job._id match {
      case Some(jobId) =>
        patch(s"$baseUrl/api/job/$jobId/state/$state").map(_.body[JsValue].asOpt[List[Job]].flatMap(_.headOption))
      case None =>
        Future.failed(new IllegalStateException("Missing job ID"))
    }
  }

  def checkoutJob(slaveID: String)(implicit ec: ExecutionContext): Future[Option[Job]] = {
    patch(s"$baseUrl/api/jobs/checkout/$slaveID").map(_.body[JsValue].asOpt[List[Job]].flatMap(_.headOption))
  }

  def createJob(job: Job)(implicit ec: ExecutionContext): Future[Option[Job]] = {
    post(s"$baseUrl/api/jobs", body = Json.toJson(job)).map(_.body[JsValue].asOpt[List[Job]].flatMap(_.headOption))
  }

  def updateStatistics(job: Job, stats: List[JobStatistics])(implicit ec: ExecutionContext): Future[Option[Job]] = {
    job._id match {
      case Some(jobId) =>
        patch(s"$baseUrl/api/job/$jobId/statistics", Json.toJson(stats))
          .map(_.body[JsValue].asOpt[List[Job]].flatMap(_.headOption))
      case None =>
        Future.failed(new IllegalStateException("Missing job ID"))
    }
  }

}
