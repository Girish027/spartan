package com.tfs.dp.spartan.utils

import java.io.{File, IOException}

import com.tfs.dp.crypto.EncryptionUtil
import com.tfs.dp.encoding.EncoderUtil
import com.tfs.dp.spartan.dg.DGCatalogModel.{Serde, TableColumn}
import com.tfs.dp.spartan.manager.CustomOutputPartitioner
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class EncoderUtilTest extends FlatSpec with Logging {

  val spark = SparkSession
    .builder()
    .appName("EncoderUtilTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val outputPath = "./outputCustom"
  val tempDirectory = "./tempOutput"
  val epochTimeStampColumn = "epochTime"
  val client = "client1"
  val account = "account1"
  val keyAccountId = "account id"
  val keyAccountData = "account (data)"
  val inputDfToEncode = Seq(
    ("client1", "account1", "1513676394000", "a"),
    ("client1", "account1", "1513676394000", "b"),
    ("client1", "account1", "1513745000000", "c"),
    ("client1", "account1", "1513745780000", "d"),
    ("client1", "account1", "1513743380000", "e"),
    ("client1", "account1", "1513743380000", "f")).toDF("client", keyAccountId, "epochTime", keyAccountData)

  it should "encode and decode the columns" in {
    CustomOutputPartitioner.write(spark, inputDfToEncode, outputPath,tempDirectory, epochTimeStampColumn, client, Serde.PARQUET)

    val reloadedDf = spark.read.parquet(outputPath + "/client=*/year=*/month=*/day=*/hour=*/min=*/")

    val decodedDf = EncoderUtil.decodeColumnNamesInDf(reloadedDf)

    val file = new File(outputPath)

    try { //Deleting the directory recursively using FileUtils.
      FileUtils.deleteDirectory(file)
      logger.info("Directory has been deleted recursively !")
    } catch {
      case e: IOException =>
        logger.error("Problem occurs when deleting the directory : " + outputPath)
        e.printStackTrace()
    }

    assert(keyAccountData.equals(decodedDf.columns(2)))
  }

}
