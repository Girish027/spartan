package com.tfs.dp.spartan.plugins.sourceConfig

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tfs.hadoop.logging.binlog.LogEventWritable
import com.tfs.dp.spartan.dg.DGCatalogModel.schemaLocation

class ActiveShareSourceAdapter extends SourceAdapterPlugin {

  override def readData(spark: SparkSession, finalPaths:Seq[String],
                        sourceConf: Map[String, String]): DataFrame = {
    val finalPath = finalPaths.mkString(",")
    val binRawData: RDD[(LogEventWritable, Text)] = spark.sparkContext.sequenceFile(finalPath, classOf[LogEventWritable],classOf[Text])
    val binRDD = binRawData.map(row => row._2.toString())
    val defaultDataDf = spark.read.json(schemaLocation +"/schema_ActiveShare.json")
    spark.sqlContext.read.schema(defaultDataDf.schema).json(binRDD)
  }
}
