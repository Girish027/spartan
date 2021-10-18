package com.tfs.dp.spartan.plugins.sourceConfig

import java.security.Security

import com.tfs.dp.spartan.conf.Consts.{ENCRYPTION_ALGORITHM, ENCRYPTION_PASSWORD}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor
import org.json.XML

class AssistTranscriptSourceAdapter extends SourceAdapterPlugin {

  override def readData(spark: SparkSession, finalPaths:Seq[String],
                        sourceConf: Map[String, String]): DataFrame = {

    //val finalPath = Utils.getFinalPathsAsString(inputPath, startDate, endDate, Consts.defaultRawDataGranularity)

    val finalPath = finalPaths.mkString(",")
    val seqRawData: RDD[(Text, Text)] = spark.sparkContext.sequenceFile(
      finalPath,
      classOf[Text],
      classOf[Text])

    val encryptedTranscripts: RDD[String] = seqRawData
      .map(x => x._2.toString)
      .map(x => x.split(" -")(1).trim)

    val transcriptJsons: RDD[String] = encryptedTranscripts.mapPartitions { transcriptsIter =>
      val encryptor = new StandardPBEStringEncryptor()
      Security.addProvider(new BouncyCastleProvider())
      encryptor.setProvider(new BouncyCastleProvider())
      encryptor.setAlgorithm(ENCRYPTION_ALGORITHM)
      encryptor.setPassword(ENCRYPTION_PASSWORD)
      val decryptedTran = transcriptsIter.map { x => {
        val xmlStr = encryptor.decrypt(x)

        val json : String = XML.toJSONObject(xmlStr).toString();
        json
      }
      }
      decryptedTran
    }
    spark.sqlContext.read.json(transcriptJsons)
  }


}
