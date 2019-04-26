package com.qwery.platform.sparksql.plugin

import scala.beans.BeanProperty

/**
  * Represents the configuration of a Spark application that is to be generated
  * @example
  * {{{
  *   <application>
  *       <appName>AdBook Client</appName>
  *       <appVersion>1.0</appVersion>
  *       <className>com.coxautoinc.maid.adbook.AdBookClientSparkJob</className>
  *       <extendsClass>com.coxautoinc.maid.WtmSparkJobBase</extendsClass>
  *       <inputPath>./apps/adbook/sql/adbook-client.sql</inputPath>
  *       <outputPath>./temp</outputPath>
  *       <classOnly>true</classOnly>
  *       <defaultDB>global_temp</defaultDB>
  *       <scalaVersion>2.11.12</scalaVersion>
  *       <sparkVersion>2.3.3</sparkVersion>
  *       <templateFile>./temp/WtmSparkJobTemplate.txt</templateFile>
  *       <properties>
  *           <property>
  *               <name>s3Path</name>
  *               <value>s3://shocktrade/financial/csv/</value>
  *           </property>
  *       </properties>
  *       <sparkProperties>
  *           <property>
  *               <name>spark.debug.maxToStringFields</name>
  *               <value>2048</value>
  *           </property>
  *           <property>
  *               <name>spark.executor.memory</name>
  *               <value>10g</value>
  *           </property>
  *       </sparkProperties>
  *   </application>
  * }}}
  * @author lawrence.daniels@gmail.com
  */
case class Application() {
  @BeanProperty var appName: String = _
  @BeanProperty var appVersion: String = _
  @BeanProperty var className: String = _
  @BeanProperty var classOnly: java.lang.Boolean = _
  @BeanProperty var defaultDB: String = _
  @BeanProperty var extendsClass: String = _
  @BeanProperty var inputPath: String = _
  @BeanProperty var outputPath: String = _
  @BeanProperty var properties: java.util.Properties = _
  @BeanProperty var scalaVersion: String = _
  @BeanProperty var sparkProperties: java.util.Properties = _
  @BeanProperty var sparkVersion: String = _
  @BeanProperty var templateFile: String = _

}