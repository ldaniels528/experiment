package com.qwery.models

/**
  * Include sources
  * @param paths the given source paths for which to load and incorporate into the current program
  */
case class Include(paths: Seq[String]) extends Invokable