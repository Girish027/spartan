package com.tfs.dp.spartan

import com.tfs.dp.exceptions.SparkSessionException
import com.tfs.dp.spartan.dg.DGCatalogModel.PhysicalTable
import com.tfs.dp.spartan.dg.{DGCatalogRegistry, TableSegmentDef}
import com.tfs.dp.spartan.manager.{DQRunner, TableManager}
import com.tfs.dp.spartan.udfs.SpartanUdfs
import com.tfs.dp.spartan.utils.PropertiesUtil
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.Set

/**
  * Created by guruprasad.gv on 8/31/17.
  */
class Spartan(rootPath: String) extends Logging {

  val spark = try{
    SparkSession.builder.appName("SpartanApp")
      .config("spark.sql.crossJoin.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()

  /*val spark = try {
    SparkSession.builder().appName("SpartanApp").config("spark.sql.crossJoin.enabled", "true").
      master("local").getOrCreate()*/
  }catch{
    case ex:Exception =>
      logger.error("Error during creating spark session", ex)
      throw SparkSessionException("Spark Session Can not be created")
  }

  PropertiesUtil.init(spark)

  SpartanUdfs.registerAll(spark)

  def catalog(): Set[String] = {
    DGCatalogRegistry.registry.keySet
  }

  def define(tableName: String): PhysicalTable = {
    DGCatalogRegistry.registry.get(tableName).get
  }

  def use(tableName: String, client: String, startPartitionId: String, endPartitionId: String, isDevMode: Boolean): TableManager = {
    new TableManager(rootPath, spark, TableSegmentDef(tableName, client, startPartitionId, endPartitionId), isDevMode)
  }

  def use(tableName: String, client: String, startPartitionId: String, endPartitionId: String): TableManager = {
    use(tableName, client, startPartitionId, endPartitionId, false)
  }

  def dqRunner(tablName: String, client: String, startPartitionId: String, endPartitionId: String): DQRunner = {
    new DQRunner(new TableSegmentDef(tablName, client, startPartitionId, endPartitionId), rootPath, spark)
  }
}
