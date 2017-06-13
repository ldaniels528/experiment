package com.github.ldaniels528.transgress.server
package dao

import com.github.ldaniels528.transgress.models.JobStates.JobState
import com.github.ldaniels528.transgress.models.{JobLike, JobStates, StatisticsLike}
import io.scalajs.npm.mongodb._

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Job DAO
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait JobDAO extends GenericDAO[JobData]

/**
  * JobDAO Companion
  * @author lawrence.daniels@gmail.com
  */
object JobDAO {

  /**
    * Job DAO enrichment
    * @param dao the given [[JobDAO data access object]]
    */
  final implicit class JobDAOEnrichment(val dao: JobDAO) extends AnyVal {

    @inline
    def checkoutJob(slaveID: String, host: String): js.Promise[FindAndModifyWriteOpResult] = {
      dao.findOneAndUpdate(
        filter = doc("state" $eq JobStates.NEW),
        update = doc($set("state" -> JobStates.CLAIMED, "processingHost" -> host, "slaveID" -> slaveID)),
        options = new FindAndUpdateOptions(sort = doc("priority" -> 1), returnOriginal = false)
      )
    }

    @inline
    def createJob(job: JobData): js.Promise[InsertWriteOpResult] = {
      val dict = job.asInstanceOf[js.Dictionary[js.Any]]
      dict("lastUpdated") = js.Date.now()
      dao.insertOne(dict, new WriteOptions())
    }

    @inline
    def changeState(jobId: String, state: JobState, message: js.UndefOr[String]): js.Promise[FindAndModifyWriteOpResult] = {
      dao.findOneAndUpdate(
        filter = doc("_id" $eq new ObjectID(jobId)),
        update = doc($set("state" -> state, "message" -> message, "lastUpdated" -> js.Date.now())),
        options = new FindAndUpdateOptions(returnOriginal = false)
      )
    }

    @inline
    def findByState(states: JobState*): Cursor[JobData] = {
      dao.find[JobData](doc("state" $in js.Array(states.map(_.toString): _*))).sort("priority", 1)
    }

    @inline
    def updateJob(job: JobData): js.Promise[UpdateWriteOpResultObject] = {
      val dict = job.asInstanceOf[js.Dictionary[js.Any]]
      dict("lastUpdated") = js.Date.now()
      dao.updateOne("_id" $eq job._id, dict)
    }

    @inline
    def updateStatistics(id: String, statistics: StatisticsLike): js.Promise[FindAndModifyWriteOpResult] = {
      dao.findOneAndUpdate(
        filter = "_id" $eq new ObjectID(id),
        update = doc($set("statistics" -> statistics, "lastUpdated" -> js.Date.now())),
        options = new FindAndUpdateOptions(returnOriginal = false)
      )
    }

  }

  /**
    * Job DAO Constructor
    * @param db the given [[Db database]] instance
    */
  final implicit class JobDAOConstructor(val db: Db) extends AnyVal {

    @inline
    def getJobDAO: JobDAO = db.collection("jobs").asInstanceOf[JobDAO]

  }

}

/**
  * Represents a job document
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class JobData(var _id: js.UndefOr[ObjectID] = js.undefined,
              var name: js.UndefOr[String],
              var input: js.UndefOr[String],
              var inputSize: js.UndefOr[Double],
              var processingHost: js.UndefOr[String],
              var slaveID: js.UndefOr[String],
              var workflowName: js.UndefOr[String],
              var state: js.UndefOr[JobState],
              var lastUpdated: js.UndefOr[Double] = js.undefined,
              var message: js.UndefOr[String] = js.undefined,
              var statistics: js.UndefOr[StatisticsLike] = js.undefined) extends JobLike