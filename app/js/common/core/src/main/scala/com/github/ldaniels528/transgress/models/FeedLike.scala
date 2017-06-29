package com.github.ldaniels528.transgress.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a feed-like model
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait FeedLike extends js.Object {

  def filename: js.UndefOr[String]

  def mtime: js.UndefOr[Double]

  def size: js.UndefOr[Double]

  def createdTime: js.UndefOr[js.Date]

}

/**
  * Feed-like Companion
  * @author lawrence.daniels@gmail.com
  */
object FeedLike {

  /**
    * Feed-like Extensions
    * @param feed the given [[FeedLike feed]]
    */
  final implicit class FileExtensions(val feed: FeedLike) extends AnyVal {

    @inline
    def elapsedTime: Double = feed.mtime.map(mtime => js.Date.now() - mtime) getOrElse 0

  }

}