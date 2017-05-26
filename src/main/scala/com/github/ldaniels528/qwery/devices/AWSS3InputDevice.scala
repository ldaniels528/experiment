package com.github.ldaniels528.qwery.devices

import java.io.InputStream

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.model.{GetObjectRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, S3ClientOptions}
import com.github.ldaniels528.qwery.devices.AWSS3InputDevice._
import com.github.ldaniels528.qwery.devices.InputDevice._
import com.github.ldaniels528.qwery.ops.Scope
import org.slf4j.LoggerFactory

import scala.io.{BufferedSource, Source}

/**
  * AWS S3 File Input Device
  * @author lawrence.daniels@gmail.com
  */
case class AWSS3InputDevice(bucketName: String, keyName: String, regionName: String) extends InputDevice {
  private lazy val log = LoggerFactory.getLogger(getClass)
  private var s3Client: Option[AmazonS3] = None
  private var s3Object: Option[S3Object] = None
  private var source: Option[BufferedSource] = None
  private var lines: Iterator[String] = Iterator.empty
  private var offset: Long = _

  override def close(): Unit = {
    source.foreach(_.close())
    s3Object.foreach(_.close())
    lines = Iterator.empty
  }

  override def getSize: Option[Long] = None

  override def open(scope: Scope): Unit = {
    val accessKeyID = scope.env("AWS_ACCESS_KEY_ID")
    val secretAccessKey = scope.env("AWS_SECRET_ACCESS_KEY")
    val clientConfiguration = new ClientConfiguration()
    clientConfiguration.setSignerOverride("AWSS3V4SignerType")
    s3Client = Option(getS3Client(accessKeyID, secretAccessKey, regionName))
    s3Object = s3Client map (getS3Object(_, bucketName, keyName))
    source = s3Object map getS3Content
    lines = source.map(_.getNonEmptyLines) getOrElse Iterator.empty
    offset = 0L
  }

  override def read(): Option[Record] = {
    if (lines.hasNext) Option {
      val bytes = lines.next().getBytes()
      statsGen.update(records = 1, bytesRead = bytes.length) foreach { stats =>
        log.info(stats.toString)
      }
      offset += 1
      Record(bytes, offset)
    } else None
  }

}

/**
  * AWS S3 File Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object AWSS3InputDevice {
  private[this] lazy val log = LoggerFactory.getLogger(getClass)

  def getS3Client(accessKeyID: String, secretAccessKey: String, regionName: String): AmazonS3 = {
    // create the S3 client
    val s3Client = AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyID, secretAccessKey)))
      .build()
    val regions = Option(Regions.fromName(regionName))
      .getOrElse(throw new IllegalArgumentException(s"Region '$regionName' not found"))
    val region = Region.getRegion(regions) // Region.getRegion(Regions.US_EAST_1)
    s3Client.setRegion(region)
    //s3Client.setEndpoint("http://localhost:9000")
    val clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).build()
    s3Client.setS3ClientOptions(clientOptions)
    s3Client
  }

  def getS3Object(s3Client: AmazonS3, bucketName: String, keyName: String): S3Object = {
    val s3Object = s3Client.getObject(new GetObjectRequest(bucketName, keyName))
    log.info(s"Content-Type: ${s3Object.getObjectMetadata.getContentType}")
    s3Object
  }

  def getS3Content(s3Object: S3Object): BufferedSource = {
    val input = s3Object.getObjectContent.asInstanceOf[InputStream]
    Source.fromInputStream(input)
  }

}