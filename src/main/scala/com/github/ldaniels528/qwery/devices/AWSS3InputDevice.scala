package com.github.ldaniels528.qwery.devices

import java.io.InputStream
import java.util.{Properties => JProperties}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.services.s3.model.{GetObjectRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.github.ldaniels528.qwery.devices.AWSS3InputDevice._
import com.github.ldaniels528.qwery.devices.InputDevice._
import com.github.ldaniels528.qwery.ops.{Hints, Scope}
import com.github.ldaniels528.qwery.util.OptionHelper._
import org.slf4j.LoggerFactory

import scala.io.{BufferedSource, Source}

/**
  * AWS S3 File Input Device
  * @author lawrence.daniels@gmail.com
  */
case class AWSS3InputDevice(bucketName: String,
                            keyName: String,
                            region: Option[String],
                            profile: Option[String],
                            config: Option[JProperties])
  extends InputDevice {
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
    val credentialsProvider = config.flatMap(getStaticCredentials).getOrElse(getProfileCredentials(profile))
    val region_? = region ?? config.flatMap(p => Option(p.getProperty("AWS_REGION")))
    val clientConfiguration = new ClientConfiguration()
    clientConfiguration.setSignerOverride("AWSS3V4SignerType")
    s3Client = Option(getS3Client(credentialsProvider, region ?? region_?))
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
object AWSS3InputDevice extends InputDeviceFactory with SourceUrlParser {
  private[this] lazy val log = LoggerFactory.getLogger(getClass)

  /**
    * Returns a compatible input device for the given URL.
    * @param url the given URL (e.g. "s3://ldaniels3/companylist.csv?region=us-west-1&profile=ldaniels3")
    * @return an option of the [[InputDevice input device]]
    */
  override def parseInputURL(url: String, hints: Option[Hints]): Option[InputDevice] = {
    val comps = parseURI(url)
    for {
      bucket <- comps.host if url.toLowerCase.startsWith("s3:")
      key <- comps.path
      region = comps.params.get("region")
      profile = comps.params.get("profile")
      config = hints.flatMap(_.properties)
    } yield AWSS3InputDevice(bucketName = bucket, keyName = key, region = region, profile = profile, config = config)
  }

  private def getProfileCredentials(profile_? : Option[String]) = {
    profile_?.map(new ProfileCredentialsProvider(_)) getOrElse new ProfileCredentialsProvider()
  }

  private def getStaticCredentials(config: JProperties): Option[AWSStaticCredentialsProvider] = {
    for {
      accessKeyID <- Option(config.getProperty("AWS_ACCESS_KEY_ID"))
      secretAccessKey <- Option(config.getProperty("AWS_SECRET_ACCESS_KEY"))
      sessionKey = config.getProperty("AWS_SESSION_TOKEN")
    } yield new AWSStaticCredentialsProvider(new BasicSessionCredentials(accessKeyID, secretAccessKey, sessionKey))
  }

  private def getS3Client(provider: AWSCredentialsProvider, regionName: Option[String]): AmazonS3 = {
    AmazonS3ClientBuilder.standard()
      .withCredentials(provider)
      .withRegion(regionName getOrElse "us-west-2")
      .build()
  }

  private def getS3Object(s3Client: AmazonS3, bucketName: String, keyName: String): S3Object = {
    val s3Object = s3Client.getObject(new GetObjectRequest(bucketName, keyName))
    log.info(s"Content-Type: ${s3Object.getObjectMetadata.getContentType}")
    s3Object
  }

  private def getS3Content(s3Object: S3Object): BufferedSource = {
    val input = s3Object.getObjectContent.asInstanceOf[InputStream]
    Source.fromInputStream(input)
  }

}