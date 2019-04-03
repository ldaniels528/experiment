package com.qwery.platform.sparksql.plugin

import scala.beans.BeanProperty

/**
  * Represents the configuration of a Spark application that is to be generated
  * @author lawrence.daniels@gmail.com
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
  *       <sparkAvroVersion>4.0.0</sparkAvro>
  *       <sparkCsvVersion>1.5.0</sparkCsv>
  *       <sparkVersion>2.3.3</sparkVersion>
  *       <templateClass>./temp/WtmSparkJobTemplate.txt</templateClass>
  *       <properties>
  *           <property>
  *               <name>spark.debug.maxToStringFields</name>
  *               <value>2048</value>
  *           </property>
  *           <property>
  *               <name>spark.executor.memory</name>
  *               <value>10g</value>
  *           </property>
  *       </properties>
  *   </application>
  * }}}
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
  @BeanProperty var sparkAvroVersion: String = _
  @BeanProperty var sparkCsvVersion: String = _
  @BeanProperty var sparkVersion: String = _
  @BeanProperty var templateClass: String = _

}