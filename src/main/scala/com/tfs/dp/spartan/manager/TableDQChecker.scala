package com.tfs.dp.spartan.manager

import java.util.Date

import com.tfs.dp.configuration.service.CatalogService
import com.tfs.dp.configuration.service.impl.CatalogServiceImpl
import com.tfs.dp.exceptions.{DQRunException, NoDQDefinedException}
import com.tfs.dp.spartan.conf.Consts.partitionIdFormater
import com.tfs.dp.spartan.dg.{DGQualityCatalogRegistry, DGTableDQDef, DQResult, TableSegmentDef}
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, DateTimeZone}

/**
 * Created by Vijay Muvva on 29/08/2018
 */
class TableDQChecker(tableSegmentDef: TableSegmentDef, sparkSession:SparkSession) extends Logging {

  private def getTableDQDef(): DGTableDQDef = {

    logger.info(s"Fetching the table DQ definition for the table: ${tableSegmentDef.tableName}")

    val dqDef = DGQualityCatalogRegistry.dqCatalog.get(tableSegmentDef.tableName)
    var tableDQDef:DGTableDQDef = null

    if (dqDef.isEmpty) {
      val catalogService: CatalogService = new CatalogServiceImpl()
      try {
        tableDQDef = catalogService.getDQDef(tableSegmentDef.tableName)
      } catch {
        case ex: NoDQDefinedException =>
          logger.error("No DQ Def returned from the catalog.")
          throw new NoDQDefinedException("No DQ Def returned from the catalog.")
        case ex: Exception =>
          logger.error(s"Error while fetching DQ definition from the catalog service for the table " +
            s"${tableSegmentDef.tableName}", ex)
          throw new DQRunException(s"Error while fetching DQ definition from the catalog service for the table " +
            s"${tableSegmentDef.tableName}")
      }
    } else {
      tableDQDef = dqDef.get
    }

    logger.info(s"Table DQ definition fetch successful for the DQ of table: ${tableSegmentDef.tableName}")

    tableDQDef
  }

  def checkDQ(): DQResult = {

    val tableDQDef = getTableDQDef()

    val startTime = partitionIdFormater.parseDateTime(tableSegmentDef.startPartition)
    val endTime = partitionIdFormater.parseDateTime(tableSegmentDef.endPartition)
    val dqEvalTime = new DateTime(new Date).withZone(DateTimeZone.UTC).toString()

    logger.info(s"Starting the DQ evaluation for the table: ${tableSegmentDef.tableName}, start time: ${startTime} end " +
      s"time: ${endTime}")

    var combinedDQDF: DataFrame = null
    for (dqDataSet <- tableDQDef.getDQDataSets()) {
      val dqDataSetDF = sparkSession.sql(dqDataSet.sql)
      var combinedRuleDF: DataFrame = null
      for (dqRule <- dqDataSet.dqRules) {
        dqDataSetDF.createOrReplaceTempView(s"${dqDataSet.id.replace("-", "")}_" +
          s"${dqRule.id.replace("-", "")}_DataSet")
        val dqSql = s"select '${ThreadContext.get("corelatId")}' as jobId, '${tableSegmentDef.tableName}' as view_id, " +
          s"'${tableSegmentDef.client}' as client, '${dqDataSet.id}' as dq_data_set_id, " +
          s"'${dqRule.id}' as rule_id, '${dqRule.name}' as rule_name, ${dqDataSet.recordIdentifier} as record_id, '${startTime}' as data_segment_time_start, " +
          s"'${endTime}' as data_segment_time_end, " +
          s"'${dqEvalTime}' as evaluation_time from " +
          s"${dqDataSet.id.replace("-", "")}_${dqRule.id.replace("-", "")}" +
          s"_DataSet where (${dqRule.sql})"

        logger.info(s"DQ SQL for the rule ${dqRule.name}: ${dqSql}")

        val ruleDF = sparkSession.sql(dqSql)
        if (combinedRuleDF == null) {
          combinedRuleDF = ruleDF
        } else {
          combinedRuleDF = ruleDF.union(combinedRuleDF)
        }
      }

      if (combinedDQDF == null) {
        combinedDQDF =  combinedRuleDF
      } else {
        combinedDQDF =  combinedRuleDF.union(combinedDQDF)
      }
    }

    combinedDQDF.persist(StorageLevel.MEMORY_AND_DISK)

    DQResult(combinedDQDF, tableDQDef.getDQDataSets(), combinedDQDF.count(), dqEvalTime)
  }
}
