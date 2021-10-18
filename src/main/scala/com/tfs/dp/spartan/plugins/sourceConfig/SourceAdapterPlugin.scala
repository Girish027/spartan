package com.tfs.dp.spartan.plugins.sourceConfig

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.logging.log4j.scala.Logging


trait SourceAdapterPlugin extends Logging {

  def readData(spark: SparkSession, finalPaths:Seq[String], sourceConf: Map[String, String]): DataFrame

}
