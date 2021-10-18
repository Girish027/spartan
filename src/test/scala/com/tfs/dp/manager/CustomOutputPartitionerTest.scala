package com.tfs.dp.manager

import com.tfs.dp.spartan.manager.CustomOutputPartitioner
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.apache.commons.io.FileUtils
import java.io.{File, IOException}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import com.tfs.dp.crypto.EncryptionUtil
import com.tfs.dp.encoding.EncoderUtil
import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.dg.DGCatalogModel.Serde

class CustomOutputPartitionerTest extends FlatSpec with Logging {

  val spark = SparkSession
    .builder()
    .appName("CustomOutputPartitionerTest")
    .master("local[*]")
    .getOrCreate();

  import spark.implicits._

  val inputDf = Seq(
    ("client1", "account1", "1513676394000", "a"),
    ("client1", "account1", "1513676394000", "b"),
    ("client1", "account1", "1513745000000", "c"),
    ("client1", "account1", "1513745780000", "d"),
    ("client1", "account1", "1513743380000", "e"),
    ("client1", "account1", "1513743380000", "f")).toDF("client", "account", "epochTime", "data")

  val outputPath = "./outputCustom"
  val tempDirectory = "./tempOutput"
  val epochTimeStampColumn = "epochTime"
  val client = "client1"
  val account = "account1"

  "Test" should "match against input against output" in {

    CustomOutputPartitioner.write(spark, inputDf, outputPath,tempDirectory, epochTimeStampColumn, client, Serde.JSON)

    val reloadedDf = spark.read.json(outputPath + "/client=*/year=*/month=*/day=*/hour=*/min=*/")

    val count1 = inputDf.count()
    val count2 = reloadedDf.count()

    // delete directory
    val file = new File(outputPath)

    try { //Deleting the directory recursively using FileUtils.
      FileUtils.deleteDirectory(file)
      logger.info("Directory has been deleted recursively !")
    } catch {
      case e: IOException =>
        logger.error("Problem occurs when deleting the directory : " + outputPath)
        e.printStackTrace()
    }

    assert(count1 === count2)
  }

  it should "count number of expected success files to be created" in {

    CustomOutputPartitioner.write(spark, inputDf, outputPath,tempDirectory, epochTimeStampColumn, client, Serde.JSON)

    val reloadedDf = spark.read.json(outputPath + "/client=*/year=*/month=*/day=*/hour=*/min=*/")

    val count1 = inputDf.count()
    val count2 = reloadedDf.count()

    val succesFilesList = Array(outputPath + "/client=client1/year=2017/month=12/day=19/hour=15/min=0/_SUCCESS",
      outputPath + "/client=client1/year=2017/month=12/day=20/hour=9/min=45/_SUCCESS",
      outputPath + "/client=client1/year=2017/month=12/day=20/hour=10/min=0/_SUCCESS",
      outputPath + "/client=client1/year=2017/month=12/day=20/hour=10/min=15/_SUCCESS",
      outputPath + "/_SUCCESS"
    )
    var count = 0
    for(i <- 0 until succesFilesList.length) {
      val file = new File(succesFilesList(i))
      if(file.isFile)
        count += 1
    }
    logger.info("Number of success files created is " + count)
    // delete directory
    val file = new File(outputPath)

    try { //Deleting the directory recursively using FileUtils.
      FileUtils.deleteDirectory(file)
      logger.info("Directory has been deleted recursively !")
    } catch {
      case e: IOException =>
        logger.error("Problem occurs when deleting the directory : " + outputPath)
        e.printStackTrace()
    }

    assert(count === 4)
  }

  it should "Encode and Decode column names for special characters for Parquet format" in {

    val keyAccountId = "account id"
    val keyAccountData = "account (data)"
    val inputDfToEncode = Seq(
      ("client1", "account1", "1513676394000", "a"),
      ("client1", "account1", "1513676394000", "b"),
      ("client1", "account1", "1513745000000", "c"),
      ("client1", "account1", "1513745780000", "d"),
      ("client1", "account1", "1513743380000", "e"),
      ("client1", "account1", "1513743380000", "f")).toDF("client", keyAccountId, "epochTime", keyAccountData)

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
