package com.tfs.dp.spartan.plugins.sourceConfig

import java.security.Security

import com.tfs.dp.spartan.conf.Consts.{ENCRYPTION_ALGORITHM, ENCRYPTION_PASSWORD}
import com.tfs.dp.spartan.dg.DGCatalogModel.schemaLocation
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor

class AssistInteractionsV2SourceAdapter extends SourceAdapterPlugin {

  override def readData(spark: SparkSession, finalPaths:Seq[String],
                        sourceConf: Map[String, String]): DataFrame = {
    val finalPath = finalPaths.mkString(",")
    val seqAssistInteractionsV2: RDD[(Text, Text)] = spark.sparkContext.sequenceFile(finalPath,classOf[Text],classOf[Text])
    val AssistInteractionsV2RDD : RDD[String] = seqAssistInteractionsV2.map(x => x._2.toString)
    val defaultDataDf = spark.read.textFile(schemaLocation+"/schema_AssistInteractionsV2.json")
    val schemaRdd = defaultDataDf.rdd
    spark.sqlContext.read.json(AssistInteractionsV2RDD ++ schemaRdd)
  }
}
