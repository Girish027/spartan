package com.tfs.dp.spartan.manager

import java.io.File

import com.tfs.dp.configuration.service.impl.CatalogServiceImpl
import com.tfs.dp.configuration.service.{CatalogService, Email, EmailService}
import com.tfs.dp.crypto.EncryptionUtil
import com.tfs.dp.exceptions.DQExportException
import com.tfs.dp.spartan.Utils
import com.tfs.dp.spartan.Utils.Granularity
import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.conf.Consts.partitionIdFormater
import com.tfs.dp.spartan.dg.DGCatalogModel.PhysicalTable
import com.tfs.dp.spartan.dg._
import com.tfs.dp.spartan.manager.CustomOutputPartitioner.cleanDirectories
import com.tfs.dp.spartan.utils.PropertiesUtil
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

import scala.util.Try

/**
 * //query - materialized only
 * //query - materialized if exist, else fallback
 * //query - query non-materialized
 *
 * //write
 * Created by guruprasad.gv on 8/26/17.
 */
class TableManager(rootPath: String, spark: SparkSession, tableSegment: TableSegmentDef, isDevMode:Boolean=false) extends Logging {

  init()

  def schema(): StructType = {
    df.schema
  }

  def df(): DataFrame = {

    val dataSetRef: String = tableSegment.tableName
    val viewDF = spark.sql(s"select * from $dataSetRef")
    viewDF.persist(StorageLevel.MEMORY_AND_DISK)
    viewDF
  }

  def getTableName: String = {
    tableSegment.tableName
  }

  def write() = {
    logger.info(s"Starting writing data for table: ${tableSegment.tableName}")
    val dataSetRef: String = tableSegment.tableName

    val table: PhysicalTable = DGCatalogRegistry.registry.get(dataSetRef).get

    val epochTimeStampColumn = table.location.epochTimeColumn

    import org.apache.spark.sql.functions.lit

    class OutputPartitionKeyColumnNotFoundException(s: String) extends Exception(s) {}

    var dfWithClient = df
    val isClientColumnPresent = df.columns.contains("client")
    if (isClientColumnPresent)
      logger.info("Client column is present in df, continuing with output partitioning")
    else {
      logger.info(s"Client column is not present in df, adding it before output partitioning")
      dfWithClient = df.withColumn("client", lit(tableSegment.client))
    }

    val tempWriteLocation = rootPath + File.separator + Consts.TEMP_WRITE_DIRECTORY

    //finding the directory path to be deleted before ingestion of data
    val path: String= (table.getPhysicalLocationUntillClient(rootPath, tableSegment.tableName)).toString().concat("/client=").concat(tableSegment.client);
    val resultantString :String = Utils.getOutputFinalPath(path,tableSegment.startPartition,tableSegment.endPartition,Granularity.FIFTEEN_MIN);
    val outputList: List[String] = resultantString.split(",").map(_.trim).toList
    logger.info(s"List of Directories to be cleaned: ${outputList}")
    //clearing of directories
    if (!(outputList.head.equals("") && outputList.length == 1)) {
      logger.info("before start deletion")
      for (dir <- outputList) {
        cleanDirectories(dir)
      }
      logger.info("after deletion completion")
    }
      CustomOutputPartitioner.write(spark, EncryptionUtil.encryptCols(spark, dfWithClient, Try(table.columns.get).getOrElse(Seq())),
        table.getPhysicalLocationUntillClient(rootPath, tableSegment.tableName),
        tempWriteLocation, epochTimeStampColumn, tableSegment.client, table.serde)
    logger.info(s"Data write successfull for table: ${tableSegment.tableName}")
  }


  def dqRunner(): DQRunner = {
    new DQRunner(tableSegment, rootPath, spark)
  }

  private def init() = {
    logger.info(s"Initialising Table Manager for table: ${tableSegment.tableName}")
    val catalogService: CatalogService = new CatalogServiceImpl()
    val table = DGCatalogRegistry.registry.get(tableSegment.tableName);
    var physicalTable: PhysicalTable = null
    if (table.isEmpty) {
      try {
        logger.info(s"Getting table definition for table: ${tableSegment.tableName} from catalog Service")
        val startPartitionOffset = DateTime.parse(tableSegment.startPartition, Consts.partitionIdFormater).getMillis.toString
        val endpartitionOffset = DateTime.parse(tableSegment.endPartition, Consts.partitionIdFormater).getMillis.toString
        physicalTable= catalogService.getViewDefinition(tableSegment.tableName, tableSegment.client, startPartitionOffset, endpartitionOffset, tableSegment.tableName, isDevMode)
        DGCatalogRegistry.registry.put(tableSegment.tableName,physicalTable)
      } catch{
        case ex: Exception =>
          logger.error(s"Error while preparing table definition for table: ${tableSegment.tableName}", ex)
          throw new IllegalArgumentException(s"Error while preparing table definition for table: ${tableSegment.tableName}", ex)
      }
    } else{
      physicalTable = table.get
    }

    physicalTable.createView(
      spark,
      tableSegment.client,
      rootPath,
      tableSegment.startPartition,
      tableSegment.endPartition,
      physicalTable.inputDataLoadStrategy,
      physicalTable.dynamicBucketPath
    )

    logger.info(s"table manager initialisation successful for table: ${tableSegment.tableName}")
  }

  def tableDQChecker(): TableDQChecker = {
    new TableDQChecker(tableSegment, spark)
  }

  def exportDQReport(dqResult: DQResult) = {

    exportToElasticSearch(dqResult, Utils.getESProps)

    exportToHdfs(dqResult)

    // Notify is the error count is more than zero
    if (dqResult.errorCount > 0) {
      logger.info(s"DQ Error count: ${dqResult.errorCount}")
      sendDQMailNotification(dqResult)
    }
  }

  def exportToElasticSearch(dqResult: DQResult, esProps:Map[String, String]) = {

    import org.apache.spark.sql.functions._
    import org.elasticsearch.spark.sql._
    import spark.implicits._

    logger.info(s"Exporting the DQ results to Elastic Search.")

    logger.info(s"Elastic Search properties: ${esProps.toString()}")


    try {
      val windowSpec = Window.partitionBy('rule_id).orderBy('rule_name)
      val dqDFLimited = dqResult.dqDF.withColumn("row_number", row_number() over windowSpec).filter($"row_number".between(1,
        PropertiesUtil.getProperty(Consts.DQ_ERROR_RECORDS_LIMIT)))

      dqDFLimited.drop("row_number").saveToEs(s"${PropertiesUtil.getProperty(Consts.ES_DQ_REPORT_DETAILED_INDEX)}/${
        PropertiesUtil.getProperty(Consts.ES_DQ_REPORT_DETAILED_INDEX_TYPE)}", esProps)

      logger.info("Exported DQ detailed report to ES successfully.")

      val startTime = partitionIdFormater.parseDateTime(tableSegment.startPartition)
      val endTime = partitionIdFormater.parseDateTime(tableSegment.endPartition)

      val dqConsolidatedDF = Seq((ThreadContext.get("corelatId"), tableSegment.tableName, tableSegment.client, startTime.toString(),
        endTime.toString(), dqResult.errorCount, if(dqResult.errorCount > 0) "FAIL" else "PASS", dqResult.dqEvalTime.toString)).toDF("JobId", "View", "Client",
        "DataSegmentStart", "DataSegmentEnd", "ErrorCount", "DQStatus",  "DQEvaluationTime")

      dqConsolidatedDF.saveToEs(s"${PropertiesUtil.getProperty(Consts.ES_DQ_REPORT_SUMMARY_INDEX)}/${
        PropertiesUtil.getProperty(Consts.ES_DQ_REPORT_SUMMARY_INDEX_TYPE)}", esProps + (Consts.ES_DQ_SUMMARY_MAPPING_ID_KEY -> "JobId"))

      logger.info("Exported DQ summary report to ES successfully.")
    } catch {
      case ex:Exception =>
        logger.error(s"Error exporting DQ report to the elastic search, ES props: ${esProps.toString()}", ex)
        throw DQExportException(s"Error exporting DQ report to the elastic search, ES props: ${esProps.toString()}")
    }

    logger.info("DQ Report exported to elastic search successfully..")
  }

  def exportToHdfs(dqResult: DQResult) = {

    logger.info("Exporting the DQ results to HDFS.")

    val table: PhysicalTable = DGCatalogRegistry.registry.get(tableSegment.tableName).get
    val dqOutputPath = (table.getPhysicalLocationUntillClient(rootPath, tableSegment.tableName)).toString().
      concat("/client=").concat(tableSegment.client) + File.separator + ThreadContext.get("corelatId") + File.separator

    logger.info(s"DQ Output directory - ${dqOutputPath}")

    dqResult.dqDF.write
      .format("json")
      .partitionBy("dq_data_set_id", "rule_id")
      .mode(SaveMode.Overwrite)
      .save(dqOutputPath)

    logger.info("DQ Result has been written to HDFS successfully.")
  }

  private def sendDQMailNotification(dqResult: DQResult) = {
    logger.info("Sending DQ mail notification.")
    EmailService.send(new Email(PropertiesUtil.getProperty(Consts.MAIL_FROM_ADDR),
      PropertiesUtil.getProperty(Consts.MAIL_TO_ADDR), composeDQMailSubject, composeDQMailBody(dqResult.dqDataSets)))
  }

  private def composeDQMailSubject(): String = {
    s"${Consts.MAIL_DQ_SUBJECT_PREFIX} ${tableSegment.client} - ${tableSegment.tableName} - " +
      s"${partitionIdFormater.parseDateTime(tableSegment.startPartition)} / " +
      s"${partitionIdFormater.parseDateTime(tableSegment.endPartition)}"
  }

  private def composeDQMailBody(dqDataSets:Seq[DQDataSet]): String = {
    var bodyHtml = ""
      //s"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"><html xmlns="http://www.w3.org/1999/xhtml"><head> <meta name="viewport" content="width=device-width"/> <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/> <title>DQ Alert</title> <style type="text/css"> .body-wrap,body{background-color:#f6f6f6}.footer,.footer a{color:#999}.aligncenter,.btn-primary{text-align:center}*{margin:0;padding:0;font-family:"Helvetica Neue",Helvetica,Helvetica,Arial,sans-serif;box-sizing:border-box;font-size:14px}.content,.content-wrap{padding:20px}img{max-width:100%}body{-webkit-font-smoothing:antialiased;-webkit-text-size-adjust:none;width:100%!important;height:100%;line-height:1.6}table td{vertical-align:top}.body-wrap{width:100%}.container{display:block!important;max-width:600px!important;margin:0 auto!important;clear:both!important}.clear,.footer{clear:both}.content{margin:0 auto;display:block}.main{background:#fff;border:1px solid #e9e9e9;border-radius:3px}.content-block{padding:0 0 20px}.header{width:100%;margin-bottom:20px}.footer{width:100%;padding:20px}.footer a,.footer p,.footer td,.footer unsubscribe{font-size:12px}h1,h2,h3{font-family:"Helvetica Neue",Helvetica,Arial,"Lucida Grande",sans-serif;color:#000;margin:40px 0 0;line-height:1.2;font-weight:400}h1{font-size:32px;font-weight:500}h2{font-size:24px}h3{font-size:18px}h4{font-size:14px;font-weight:600}ol,p,ul{margin-bottom:10px;font-weight:400}ol li,p li,ul li{margin-left:5px;list-style-position:inside}a{color:#ee8720;text-decoration:underline}.alert a,.btn-primary{text-decoration:none}.btn-primary{color:#FFF;background-color:#ee8720;border:solid #ee8720;border-width:5px 10px;line-height:2;font-weight:700;cursor:pointer;display:inline-block;border-radius:5px;text-transform:capitalize}.alert,.alert a{color:#fff;font-weight:500;font-size:16px}.last{margin-bottom:0}.first{margin-top:0}.alignright{text-align:right}.alignleft{text-align:left}.alert{padding:20px;text-align:center;border-radius:3px 3px 0 0}.alert.alert-warning{background:#f8ac59}.alert.alert-bad{background:#ed5565}.alert.alert-good{background:#ee8720}.invoice{margin:40px auto;text-align:left;width:80%}.invoice td{padding:5px 0}.invoice .invoice-items{width:100%}.invoice .invoice-items td{border-top:#eee 1px solid}.invoice .invoice-items .total td{border-top:2px solid #333;border-bottom:2px solid #333;font-weight:700}@media only screen and (max-width:640px){.container,.invoice{width:100%!important}h1,h2,h3,h4{font-weight:600!important;margin:20px 0 5px!important}h1{font-size:22px!important}h2{font-size:18px!important}h3{font-size:16px!important}.content,.content-wrap{padding:10px!important}}</style></head><body><table class="body-wrap"> <tr> <td></td><td width="600"> <br><table class="main" width="100%" cellpadding="0" cellspacing="0"> <tr> <td class="alert alert-good"> A DQ violation has been detected..! </td></tr><tr> <td class="content-wrap"> <table width="100%" cellpadding="0" cellspacing="0"> <tr> <td class="content-block"> <table style="border-collapse: collapse;"> <tbody> <tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>View:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> ${tableSegment.tableName} </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>Client:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> ${tableSegment.client} </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>Time&nbsp;Range:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> ${partitionIdFormater.parseDateTime(tableSegment.startPartition)} - ${partitionIdFormater.parseDateTime(tableSegment.endPartition)} </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>Evaluation&nbsp;Time:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> ${new DateTime(new Date).withZone(DateTimeZone.UTC).toString()} </td></tr>"""

    /*for (dqDataSet <- dqDataSets) {
      bodyHtml = bodyHtml + """<tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>DQ&nbsp;Data&nbsp;Set&nbsp;Name:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> """
      bodyHtml = bodyHtml + dqDataSet.name + " </td></tr>"
      bodyHtml = bodyHtml + """<tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>Data Set SQL:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;">"""
      bodyHtml = bodyHtml + dqDataSet.sql + " </td></tr>"
      bodyHtml = bodyHtml + """ </td></tr><tr> <td style="border: 1px solid orange;padding: 10px;text-align: left;"> <strong>DQ Rules:</strong> </td><td style="border: 1px solid orange;padding:9px;text-align: left;"> """

      var firstRule = true
      for (dqRule <- dqDataSet.dqRules) {

        if (firstRule) {
          bodyHtml = bodyHtml + dqRule.sql.replace("<", "&lt;").replace(">",
            "&gt;").replace("&", "&amp;")
          firstRule = false
        } else {
          bodyHtml = bodyHtml + ", <br>" + dqRule.sql.replace("<", "&lt;").replace(">",
            "&gt;").replace("&", "&amp;")
        }
      }
      bodyHtml = bodyHtml + s""" </td></tr></tbody></table> </td></tr><tr> <td class="content-block"> <a href="${PropertiesUtil.getProperty(Consts.DQ_SUMMARY_REPORT_URL)}" class="btn-primary">View detailed report</a> </td></tr></table> </td></tr></table> </td><td></td></tr></table></body></html>"""
    }*/
    val DQ_SUMMARY_REPORT_URL_OOZIE_ID = PropertiesUtil.getProperty(Consts.DQ_SUMMARY_REPORT_URL).format(ThreadContext.get("corelatId"))
    val DQ_DETAILED_REPORT_URL_OOZIE_ID = PropertiesUtil.getProperty(Consts.DQ_DETAIL_REPORT_URL).format(ThreadContext.get("corelatId"))
    bodyHtml = bodyHtml + s""" </tbody></table> </td></tr><tr><td class="content-block"><a href="${DQ_SUMMARY_REPORT_URL_OOZIE_ID}" class="btn-primary">View summary report</a></td></tr><tr> <td class="content-block"> <a href="${DQ_DETAILED_REPORT_URL_OOZIE_ID}" class="btn-primary">View detailed report</a></td></tr></table> </td></tr></table> </td></tr></table></body></html>"""
    bodyHtml
  }
}
