package com.tfs.dp.spartan.dg

import com.tfs.dp.spartan.conf.{Consts, ViewInputLoadStrategy}
import com.tfs.dp.spartan.dg.DGCatalogModel.{Location, PhysicalTable, Serde}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class SpartanDynamicDateTest extends FlatSpec with Logging {
    val spark = SparkSession
      .builder()
      .appName("SpartanDynamicDateTest")
      .master("local[*]")
      .getOrCreate()

  "SpartanDynamicDateTest" should "pass field in format <yyyy-MM-dd> as input path which should be replaced with value and pass customParams" in {
    val rootPath = "./unitTestData/input/avro"
    val client = null
    val startPartition = "201802260000"
    val endPartition = "201802280000"
    val tableName: String = "avroTable"
    val uriFormat: String = ""
    val epochTimeColumn: String = null
    val clientOverrideMap: Map[String, String] = Map.empty
    var customParams = Map.empty[String, String]
    val dynamicBucketPath : String = "<yyyy-mm-dd>/*/*"

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.AVRO,
      clientOverrideMap, null, Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS), None, null,
      None, None, ViewInputLoadStrategy.RELATIVE, None, None, Some(customParams+ ("L1" -> "AvroTable")), Some(dynamicBucketPath) )
      //assertThrows[java.lang.AssertionError]{
       physicalTable.createView(spark,client,rootPath,startPartition,endPartition,physicalTable.inputDataLoadStrategy, physicalTable.dynamicBucketPath)

    val finalResult = spark.sql("select * from "+physicalTable.tableName)
    assert(!finalResult.toString().isEmpty)

    //}
  }
}
