package com.tfs.dp.spartan

import com.tfs.dp.exceptions.NoDQDefinedException
import com.tfs.dp.spartan.manager.TableManager
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging

/**
 * Created by guruprasad.gv on 9/25/17.
 */
object SpartanDQCheckApp extends Logging {

  def main(args: Array[String]): Unit = {

    /*val tableName: String = args(0)
    val client: String = args(1)
    val rootPath: String = args(2)
    val startPartitionId: String = args(3)
    val endPartitionId: String = args(4)
    val spartan: Spartan = new Spartan(rootPath);
    //val dqManager: DQRunner = spartan.createDQManager(tableName, client, startPartitionId, endPartitionId);
    //dqManager.runDQ();*/
    val version: String = getClass.getPackage.getSpecificationVersion

    if (5 != args.length) throw new IllegalArgumentException("Required arguments(view name, client, root path, start, " +
      "end time, isDevMode(optional) and isSleepRequired(optional))")

    val tableName: String = args(0)
    val client: String = args(1)
    val rootPath: String = args(2)
    val startPartitionId: String = args(3)
    val endPartitionId: String = args(4)
    val isDevMode: Boolean = if (6 <= args.length) args(5).toBoolean else false
    val isSleepRequired: Boolean = if (7 == args.length) args(6).toBoolean else false
    val wfId = System.getProperty("oozie.job.id")

    ThreadContext.put("client", client)
    ThreadContext.put("view", tableName)
    ThreadContext.put("corelatId", wfId)
    ThreadContext.put("service", "DQCheck")

    SpartanMaterializationApp.validateStartAndEndTimes(startPartitionId, endPartitionId)

    logger.info(s"Starting DQ check for : view name $tableName , client $client, root path $rootPath, start time " +
      s"$startPartitionId, end time $endPartitionId version $version")

    try {
      val spartan: Spartan = new Spartan(rootPath);
      val tableMgr: TableManager = spartan.use(tableName, client, startPartitionId, endPartitionId, isDevMode);
      logger.info("Invoking the table DQ checker..")
      val dqResult = tableMgr.tableDQChecker().checkDQ()
      tableMgr.exportDQReport(dqResult)
      logger.info(s"Table DQ check complete for view: $tableName, client: $client, startDate $startPartitionId, " +
        s"endDate: $endPartitionId")
      if(isSleepRequired){
        logger.info("Sleeping for 1 hr to enable debugging; Press ctrl+C to exit")
        Thread.sleep(3600000L)
      }
    } catch {
      case ex:NoDQDefinedException =>
        logger.error(s"DQ check stopped as there is no DQ definition found against the view ${tableName}")
      case ex:Throwable => logger.error(s"Error during table dq check: $tableName", ex)
        throw ex;
    }
  }
}
