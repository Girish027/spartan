package com.tfs.dp.spartan.dg

import java.io.{File, IOException}

import com.tfs.dp.spartan.conf.{Consts, ViewInputLoadStrategy}
import com.tfs.dp.spartan.dg.DGCatalogModel.{Location, PhysicalTable, Serde}
import com.tfs.dp.spartan.manager.CustomOutputPartitioner
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class DGCatalogModelTest extends FlatSpec with Logging {

  val spark = SparkSession
    .builder()
    .appName("CustomOutputPartitionerTest")
    .master("local[*]")
    .getOrCreate();

  import spark.implicits._

  val outputPath = "./outputCustom"
  val tempDirectory = "./tempOutput"
  val epochTimeStampColumn = "epochTime"
  val client = "client1"
  val account = "account1"

  val startPartition = "201712191500"
  val endPartition = "201712200900"
  val tableName: String = "avroTable"
  val uriFormat: String = ""
  val epochTimeColumn: String = null
  val clientOverrideMap: Map[String, String] = Map.empty
  var customParams = Map.empty[String, String]
  val dynamicBucketPath : String = "<yyyy-mm-dd>/*/*"

  it should "read materialized Parquet data for encoded columns" in {

    val inputDf = Seq(
      ("client1", "account1", "1513676394000", "a"),
      ("client1", "account1", "1513676394000", "b"),
      ("client1", "account1", "1513745000000", "c"),
      ("client1", "account1", "1513745780000", "d"),
      ("client1", "account1", "1513743380000", "e"),
      ("client1", "account1", "1513743380000", "f")).toDF("client", "account id", "epochTime", "data")

    CustomOutputPartitioner.write(spark, inputDf, outputPath+"/view="+tableName,tempDirectory, epochTimeStampColumn, client, Serde.PARQUET)

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.PARQUET,
      clientOverrideMap, null, Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS), None, null,
      Some(true), Some(true), ViewInputLoadStrategy.RELATIVE, None, None, Some(customParams+ ("L1" -> "AvroTable")), Some(dynamicBucketPath) )

    physicalTable.createView(spark,client,outputPath,startPartition,endPartition,physicalTable.inputDataLoadStrategy, physicalTable.dynamicBucketPath)

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

    val finalResult = spark.sql("select * from "+physicalTable.tableName)
    assert(!finalResult.toString().isEmpty)

  }

}
