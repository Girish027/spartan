package com.tfs.dp.spartan.plugins.sourceConfig

import java.security.Security
import com.tfs.dp.spartan.conf.Consts.{ENCRYPTION_ALGORITHM, ENCRYPTION_PASSWORD}
import com.tfs.dp.spartan.dg.DGCatalogModel.schemaLocation
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor

class AssistAgentStatsSourceAdapter extends SourceAdapterPlugin {

  override def readData(spark: SparkSession, finalPaths:Seq[String],
                        sourceConf: Map[String, String]): DataFrame = {

    //val finalPath = Utils.getFinalPathsAsString(inputPath, startDate, endDate, Consts.defaultRawDataGranularity)
    val finalPath = finalPaths.mkString(",")

    val seqRawData: RDD[(Text, Text)] = spark.sparkContext.sequenceFile(
      finalPath,
      classOf[Text],
      classOf[Text])

    val encryptedInteractions: RDD[String] = seqRawData
      .map(x => x._2.toString)
      .map(x => x.split(" -")(1).trim)

    val agentStatsJsons: RDD[String] = encryptedInteractions.mapPartitions { agentStatsIter =>
      val encryptor = new StandardPBEStringEncryptor()

      Security.addProvider(new BouncyCastleProvider())
      encryptor.setProvider(new BouncyCastleProvider())
      encryptor.setAlgorithm(ENCRYPTION_ALGORITHM)
      encryptor.setPassword(ENCRYPTION_PASSWORD)
      val decryptedIter = agentStatsIter.map { x => encryptor.decrypt(x) }
      decryptedIter
    }
    val defaultDataDf = spark.read.textFile(schemaLocation+"/schema_AssistAgentStats.json")
    val schemaRdd = defaultDataDf.rdd
    spark.sqlContext.read.json(agentStatsJsons ++ schemaRdd)
  }
}

