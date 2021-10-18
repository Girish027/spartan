package com.tfs.dp.spartan.plugins.sql

import com.tfs.dp.spartan.dg.DGCatalogModel.{PhysicalTable, SQLTableBuilderPlugInConf}
import com.tfs.dp.spartan.plugins.SpartanProcessorPlugin
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

class GenericSparkSQLPlugin extends SpartanProcessorPlugin with Logging{
  override def process(sparkSession:SparkSession, tableDef:PhysicalTable):Unit = {
    tableDef.tableBuilder.get.tableBuilderPlugInConf.asInstanceOf[SQLTableBuilderPlugInConf].sqls.foreach(sql => {
      logger.info(s"Defining sql view: ${sql.outputRef} for sql: ${sql.sql}")
      val dfTemp = sparkSession.sql(sql.sql)
      dfTemp.createOrReplaceTempView(sql.outputRef)
      logger.info(s"SQL view: ${sql.outputRef} defined successfully")
    })
  }
}
