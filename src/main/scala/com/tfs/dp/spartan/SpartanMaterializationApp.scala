package com.tfs.dp.spartan

import com.tfs.dp.exceptions.{DQRunException, NoDQDefinedException}
import com.tfs.dp.spartan.conf.Consts
import com.tfs.dp.spartan.dg.DGCatalogModel.PhysicalTable
import com.tfs.dp.spartan.dg.{DGCatalogModel, DGCatalogRegistry}
import com.tfs.dp.spartan.manager.TableManager
import com.tfs.dp.spartan.utils.kafka.KafkaWriter
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging
import org.joda.time.DateTime
/**
  *
  * Eg input :
  *
  * "ResolutionReport", "ebay","rootPath e.g : / " "201707150000", "201707150000"
  *
  */
object SpartanMaterializationApp extends Logging{
  def main(args: Array[String]): Unit = {
    val version: String = getClass.getPackage.getSpecificationVersion
    if(5 != args.length && 6 != args.length){
      throw new IllegalArgumentException(s"Required arguments(view name, client, path, start, end time, isDevMode(optional) and isSleepRequired(optional))")
    }else {
      val tableName: String = args(0)
      val client: String = args(1)
      val rootPath: String = args(2)
      val startPartitionId: String = args(3)
      val endPartitionId: String = args(4)
      val isDevMode: Boolean = if (6 <= args.length) args(5).toBoolean else false
      val isSleepRequired: Boolean = if (7 == args.length) args(6).toBoolean else false

      var wfId = System.getProperty(Consts.OOZIE_JOB_ID)
      val nominaltime = System.getProperty(Consts.NOMINAL_TIME)

      //In case of adhoc run there will be no Oozie job Id
      if(null == wfId || wfId.isEmpty){
        wfId = "adhoc-"+System.currentTimeMillis()
      }

      ThreadContext.put(Consts.CLIENT_NAME, client)
      ThreadContext.put(Consts.VIEW_NAME, tableName)
      ThreadContext.put(Consts.OOZIE_JOB_ID, wfId)
      ThreadContext.put("corelatId", wfId)
      ThreadContext.put("service", "ProcessorTask")
      ThreadContext.put(Consts.NOMINAL_TIME, nominaltime)

      logger.debug(s"wfId - corelatId and oozie.job.id is :${ThreadContext.get("corelatId")} and ${ThreadContext.get(Consts.OOZIE_JOB_ID)}")

      validateStartAndEndTimes(startPartitionId, endPartitionId)

      logger.info(s"Starting App for : view name $tableName , client $client, root path $rootPath, start time $startPartitionId, end time $endPartitionId version $version")
      try {
        val spartan: Spartan = new Spartan(rootPath);
        val tableMgr: TableManager = spartan.use(tableName, client, startPartitionId, endPartitionId, isDevMode);

        val table: PhysicalTable = DGCatalogRegistry.registry.get(tableMgr.getTableName).get

        val dqEnabled = table.dqEnable.get
        val isMaterializationEnabled = table.materializationEnabled.get

        if(isMaterializationEnabled){
          logger.info(s"Starting Materialization for view ${tableName}")
          tableMgr.write()
          logger.info(s"Materialisation complete for view: $tableName, client: $client, startDate $startPartitionId, endDate: $endPartitionId")
        } else {
          logger.info(s"Skipping materialization for view ${tableName} as it is disabled")
        }

        if(dqEnabled){
          logger.info(s"Starting DQ for view: ${tableName} ")
          Utils.updateDQStatusInSummaryIndex(spartan.spark,"In progress")
          try {
            logger.info("Invoking the table DQ checker..")
            val dqResult = tableMgr.tableDQChecker().checkDQ()
            tableMgr.exportDQReport(dqResult)
            logger.info("Table DQ check successful..")
          } catch {
            case ex: NoDQDefinedException =>
              logger.warn(s"DQ execution stopped as no DQ definition found against the view ${tableName}")
              Utils.updateDQStatusInSummaryIndex(spartan.spark,"ERROR")
            case ex: DQRunException =>
              logger.warn(s"DQ Run failed for view $tableName", ex)
              Utils.updateDQStatusInSummaryIndex(spartan.spark,"ERROR")
            case ex: Throwable =>
              logger.error(s"Error while processing DQ for view $tableName", ex)
              Utils.updateDQStatusInSummaryIndex(spartan.spark,"ERROR")
          }
        } else{
          logger.info(s"Skipping DQ for view: $tableName as it is disabled.")
          Utils.updateDQStatusInSummaryIndex(spartan.spark,"NA")
        }

        if(isSleepRequired){
          logger.info("Sleeping for 1 hr to enable debugging; Press ctrl+C to exit")
          Thread.sleep(3600000L)
        }
      } catch {
        case ex:Throwable => logger.error(s"Error during materializing table: $tableName", ex)
          throw ex;
      }
      finally {
        DGCatalogModel.pushMetricsToKafka
        KafkaWriter.closeProducer
      }
    }
  }

  def validateStartAndEndTimes(startPartitionId: String, endPartitionId: String): Unit = {
    var startTime:DateTime =null
    var endTime:DateTime = null
    logger.info("validating start and end Time format")
    //check whether they are numeric
    try{
      startTime = DateTime.parse(startPartitionId, Consts.partitionIdFormater)
      endTime = DateTime.parse(endPartitionId, Consts.partitionIdFormater)
    } catch {
      case illegalArgEx : IllegalArgumentException =>
        logger.error("Error while parsing startTime and EndTime to format "+Consts.partitionIdFormater, illegalArgEx)
        throw illegalArgEx;
    }
    if(startTime.isAfter(endTime)){
      logger.error(s"StartTime provided: $startPartitionId is greater than end Time: $endPartitionId")
      throw new IllegalArgumentException(s"StartTime provided: $startPartitionId is greater than end Time: $endPartitionId")
    }
    if(!Utils.checkValidMinuteValueForRawDataPartition(startTime) || !Utils.checkValidMinuteValueForRawDataPartition(endTime)){
      logger.error("Minute Value Supplied with startTime or end Time is inValid")
      throw new IllegalArgumentException("Minute Value Supplied with startTime or end Time is inValid")
    }
    logger.info("validation successful for start and end Time format")
  }
}
