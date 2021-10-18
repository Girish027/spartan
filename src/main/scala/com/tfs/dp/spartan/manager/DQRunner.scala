package com.tfs.dp.spartan.manager

import java.sql.Date

import com.tfs.dp.spartan.conf.Consts._
import com.tfs.dp.spartan.dg.DGCatalogModel.ImportStatic
import com.tfs.dp.spartan.dg._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.DateTime

/**
  * Created by guruprasad.gv on 9/25/17.
  */
class DQRunner(tableSegmentDef: TableSegmentDef,
               rootPath: String,
               spark: SparkSession

              ) {


  def runDQ(): DataFrame = {

    import spark.implicits._

    val dqDefinition: DGQualityDefinition =
      DGQualityCatalog.getDQDefinition(tableSegmentDef.tableName, tableSegmentDef.client);

    val tabDQ = dqDefinition.getTableDQ();

    val startTime = partitionIdFormater.parseDateTime(tableSegmentDef.startPartition);
    val endTime = partitionIdFormater.parseDateTime(tableSegmentDef.endPartition);

    var combinedDataSet: DataFrame = null;

    tabDQ.testDataSets.foreach(tds => {

      var inputDf: DataFrame = createInputDF(tds.inputReader, startTime, endTime)

      val resultFunctionUdf = udf((r: Row) => {

        val results = tds.tests.map(test => {
          val res: Result = test.testFunction(r)
          TestResult(tds.testDataSetName, test.testName, test.testDesc, res.pass, res.description)
        })
        val overallPass: Boolean = results.map(a => a.pass).foldLeft(true)(_ && _)

        TableResult(overallPass, results)

      });

      val rowDataUdf = udf((r: Row) => {
        r.mkString("|")
      })

      inputDf = inputDf.withColumn(s"rowData", rowDataUdf(struct(inputDf.columns.map(inputDf(_)): _*)))
      inputDf = inputDf.withColumn(s"testDataSet", lit(tds.testDataSetName))
      inputDf = inputDf.withColumn(s"testDataSetSQL", lit(tds.inputReader.sql))
      inputDf = inputDf.withColumn(s"startTime", lit(new Date(startTime.getMillis)))
      inputDf = inputDf.withColumn(s"endTime", lit(new Date(endTime.getMillis)))
      inputDf = inputDf.withColumn(s"client", lit(tableSegmentDef.client))
      inputDf = inputDf.withColumn(s"table", lit(tableSegmentDef.tableName))
      inputDf = inputDf.withColumn(s"result", resultFunctionUdf(struct(inputDf.columns.map(inputDf(_)): _*)))
      inputDf = inputDf.withColumn(s"dqPass", $"result.dqPass")

      inputDf.createOrReplaceTempView(s"DQResult_${tableSegmentDef.tableName}_${tds.testDataSetName}")

      val dqProjection = inputDf.select("table", "client", "testDataSet", "testDataSetSQL", "startTime", "endTime", "rowData", "result", "dqPass");
      if (combinedDataSet == null) {
        combinedDataSet = dqProjection;
      } else {
        combinedDataSet = dqProjection.union(combinedDataSet)
      }
    })

    tabDQ.monitoredMetrics.foreach(mm => {
      var mmDf = createInputDF(mm.inputReader, startTime, endTime)
      //Todo create anomaly metrics
    })
    combinedDataSet
  }


  private def createInputDF(inputReader: InputReader, startTime: DateTime, endTime: DateTime) = {
    inputReader.imports.foreach(imprt => {
      val importedTableManager: TableManager = new TableManager(rootPath, spark,
        new TableSegmentDef(
          imprt.asInstanceOf[ImportStatic].table.tableName,
          tableSegmentDef.client,
          imprt.asInstanceOf[ImportStatic].startPartition,
          imprt.asInstanceOf[ImportStatic].endPartition
        ))
    })
    spark.sql(inputReader.sql)
  }
}
