package com.tfs.dp

import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.dg.DGCatalogModel._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest._


/**
  * Created by Srinidhi.bs on 11/26/17.
  */
class AvroDedupTest extends FlatSpec with Logging{

  val spark = SparkSession
    .builder()
    .appName("AvroDedupTest")
    .master("local[*]")
    .getOrCreate()

  "Test" should "verify avro deduplication" in {

    val rootPath = "./unitTestData/input/avro"

    val client = null
    val startPartition = "201802270000"
    val endPartition = "201802270000"
    val tableName: String = "avroTable"
    val uriFormat: String = ""
    val epochTimeColumn: String = null
    val deduplication: RawEventDeduplicator = RawEventDeduplicator(Seq("id1", "id2"))
    val clientOverrideMap: Map[String, String] = Map.empty

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.AVRO,
      clientOverrideMap,null, Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS) ,None, deduplication)

    physicalTable.createView(
      spark,
      client,
      rootPath,
      startPartition,
      endPartition,
      Consts.DEFAULT_LOAD_STATEGY, Some(""))
    val sqlDF = spark.sql("SELECT * FROM avroTable")

    val numberOfRows = sqlDF.count()
    println("Number of rows after deduplication is " + numberOfRows)


    // test
    assert(numberOfRows === 3)
  }

  it should "verify assist interactions deduplication" in {

    val rootPath = "./unitTestData/input/assistInter"

    val client = null
    val startPartition = "201802270000"
    val endPartition = "201802270000"
    val tableName: String = "assistInteractionsTable"
    val uriFormat: String = ""
    val epochTimeColumn: String = null
    val deduplication: RawEventDeduplicator = RawEventDeduplicator(Seq("eventRaisedTimeMillis", "eventId"))
    val clientOverrideMap: Map[String, String] = Map.empty

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.ASSIST_INTERACTIONS,
      clientOverrideMap, null, Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS),None, deduplication)

    physicalTable.createView(
      spark,
      client,
      rootPath,
      startPartition,
      endPartition,
      Consts.DEFAULT_LOAD_STATEGY, Some(""))
    val sqlDF = spark.sql("SELECT * FROM assistInteractionsTable")

    val numberOfRows = sqlDF.count()
    println("Number of rows after deduplication is " + numberOfRows)


    // test
    assert(numberOfRows === 3)
  }

  it should "verify assist transcripts deduplication" in {

    val rootPath = "./unitTestData/input/assistTrans"

    val client = null
    val startPartition = "201802270000"
    val endPartition = "201802270000"
    val tableName: String = "assistTransTable"
    val uriFormat: String = ""
    val epochTimeColumn: String = null
    val deduplication: RawEventDeduplicator = RawEventDeduplicator(Seq("message.id"))
    val clientOverrideMap: Map[String, String] = Map.empty

    var physicalTable: PhysicalTable = PhysicalTable(tableName,
      Location(uriFormat, epochTimeColumn), Serde.ASSIST_TRANSCRIPTS,
      clientOverrideMap, null, Some(Consts.GENERIC_SOURCE_PLUGIN_MAIN_CLASS), None, deduplication)

    physicalTable.createView(
      spark,
      client,
      rootPath,
      startPartition,
      endPartition,
      Consts.DEFAULT_LOAD_STATEGY, Some(""))
    val sqlDF = spark.sql("SELECT * FROM assistTransTable")

    val numberOfRows = sqlDF.count()
    println("Number of rows after deduplication is " + numberOfRows)


    // test
    assert(numberOfRows === 4)
  }

}
