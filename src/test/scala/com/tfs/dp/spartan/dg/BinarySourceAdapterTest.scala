package com.tfs.dp.spartan.dg

import com.tfs.dp.spartan.conf.{Consts, ViewInputLoadStrategy}
import com.tfs.dp.spartan.dg.DGCatalogModel.{Location, PhysicalTable, Serde}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest._

class BinarySourceAdapterTest extends FlatSpec with Logging {
  val spark = SparkSession
    .builder()
    .appName("BinarySourceAdapterTest")
    .master("local[*]")
    .getOrCreate();

  "ReadBinaryData" should "verify reading of binary data" in {
    val rootPath = "./unitTestData/input/binlog"
    val client = null
    val startPartition = "201802270000"
    val endPartition = "201802270000"
    val tableName: String = "avroTable"
    val uriFormat: String = ""
    val epochTimeColumn: String = null
    val clientOverrideMap: Map[String, String] = Map.empty

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.ACTIVE_SHARE,
      clientOverrideMap, null, Some(Consts.ActiveShare_SOURCE_ADAPTER_MAIN_CLASS), None, null,
      None, None, ViewInputLoadStrategy.STATIC, None, None)
      physicalTable.createView(spark,client,rootPath,startPartition,endPartition,physicalTable.inputDataLoadStrategy, physicalTable.dynamicBucketPath)
      val sqlDF = spark.sql("select * from avroTable")
      assert(sqlDF != null)
  }
  "ReadSessionizedBinLogs2" should "verify reading of sessionized binlog data" in {
    val rootPath = "./unitTestData/input/sessionizedBinlog"
    val client = null
    val startPartition = "201802270000"
    val endPartition = "201802270000"
    val tableName: String = "avroTable"
    val uriFormat: String = ""
    val epochTimeColumn: String = null
    val clientOverrideMap: Map[String, String] = Map.empty

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.SESSIONIZED_BINLOG,
      clientOverrideMap, null, Some(Consts.Sessionized_Binlog_SOURCE_ADAPTER_MAIN_CLASS), None, null,
      None, None, ViewInputLoadStrategy.STATIC, None, None)
    physicalTable.createView(spark, client, rootPath, startPartition, endPartition, physicalTable.inputDataLoadStrategy, physicalTable.dynamicBucketPath)
    val sqlDF = spark.sql("SELECT * FROM avroTable")
    assert(sqlDF != null)
  }
}
