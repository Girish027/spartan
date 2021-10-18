package com.tfs.dp.spartan

import com.tfs.dp.spartan.conf.{Consts, ViewInputLoadStrategy}
import com.tfs.dp.spartan.dg.DGCatalogModel.{Location, PhysicalTable, Serde}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest._

class InputLoadStrategyTest extends FlatSpec with Logging {
  val spark = SparkSession
    .builder()
    .appName("InputLoadStrategyTest")
    .master("local[*]")
    .getOrCreate();

  "InputLoadStrategyTest1" should "throw exception for invalid input paths to load while InputLoadStrategy = STATIC" in {
    val rootPath = "./unitTestData/input/"
    val client = null
    val startPartition = "201802270000"
    val endPartition = "201802270000"
    val tableName: String = "avroTable"
    val uriFormat: String = ""
    val epochTimeColumn: String = null
    val clientOverrideMap: Map[String, String] = Map.empty

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.AVRO,
      clientOverrideMap, null, Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS), None, null,
      None, None, ViewInputLoadStrategy.STATIC, None, None)
    assertThrows[java.lang.AssertionError]{
      physicalTable.createView(spark,client,rootPath,startPartition,endPartition,physicalTable.inputDataLoadStrategy, physicalTable.dynamicBucketPath)
    }
  }

  "InputLoadStrategyTest2" should "verify input paths to load for loadStrategy value being STATIC" in {
    val rootPath = "./unitTestData/input/avro/static/"
    val client = null
    val startPartition = "201802270000"
    val endPartition = "201802270000"
    val tableName: String = "avroTable"
    val uriFormat: String = ""
    val epochTimeColumn: String = null
    val clientOverrideMap: Map[String, String] = Map.empty

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.AVRO,
      clientOverrideMap, null, Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS), None, null,
      None, None, ViewInputLoadStrategy.STATIC, None, None)
    physicalTable.createView(spark,client,rootPath,startPartition,endPartition,physicalTable.inputDataLoadStrategy,physicalTable.dynamicBucketPath)
    val sqlDF = spark.sql("SELECT * FROM avroTable")
    assert(sqlDF != null)
  }
}
