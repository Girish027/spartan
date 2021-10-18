package com.tfs.dp.spartan.plugins.sourceConfig

import com.tfs.dp.spartan.dg.DGCatalogModel.schemaLocation
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class JSONSeqSourceAdapter extends SourceAdapterPlugin {
  override def readData(spark: SparkSession, finalPaths:Seq[String],
                        sourceConf: Map[String, String]): DataFrame = {

    val finalPath = finalPaths.mkString(",")
    val seqRawData: RDD[(Text, Text)] = spark.sparkContext.sequenceFile(
      finalPath,
      classOf[Text],
      classOf[Text])

    val tickets: RDD[String] = seqRawData
      .map(x => x._2.toString)

    val defaultDataDf = spark.read.textFile(schemaLocation+"/schema_AssistTickets.json")
    val schemaRdd = defaultDataDf.rdd
    spark.sqlContext.read.json(tickets ++ schemaRdd)
  }
}
