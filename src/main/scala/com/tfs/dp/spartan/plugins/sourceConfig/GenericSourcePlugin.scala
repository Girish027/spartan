package com.tfs.dp.spartan.plugins.sourceConfig
import org.apache.spark.sql.SparkSession

class GenericSourcePlugin extends SourceAdapterPlugin {

  override def readData(spark: SparkSession, finalPaths: Seq[String], sourceConf: Map[String, String]) = ???

}
