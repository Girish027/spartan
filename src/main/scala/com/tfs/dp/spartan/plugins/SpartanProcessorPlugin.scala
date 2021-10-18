package com.tfs.dp.spartan.plugins

import com.tfs.dp.spartan.dg.DGCatalogModel.PhysicalTable
import org.apache.spark.sql.SparkSession

trait SpartanProcessorPlugin {

  def process(sparkSession:SparkSession, tableDef:PhysicalTable):Unit
}
