package com.tfs.dp.spartan.dg

import com.tfs.dp.spartan.dg.DGCatalogModel.Import


/**
  * Created by guruprasad.gv on 9/24/17.
  */
class DGQualityDefinition(tableDQ: TableDQ) {

  def getTableDQ(): TableDQ = {
    tableDQ
  }
}

case class Result(pass: Boolean, description: String)

case class TestResult(testDateSetName: String, testName: String, testDesc: String, pass: Boolean, description: String)

case class TableResult(dqPass: Boolean, tests: Seq[TestResult]);

case class Test(
                 testName: String,
                 testDesc: String,
                 testFunction: org.apache.spark.sql.Row => Result
               )

case class InputReader(
                        imports: Seq[Import],
                        sql: String
                      )

case class TestDataSet(

                        testDataSetName: String,
                        inputReader: InputReader,
                        tests: Seq[Test]
                      )

case class TableDQ(
                    tableName: String,
                    testDataSets: Seq[TestDataSet],
                    monitoredMetrics: Seq[MonitoredMetric]
                  )

case class MonitoredMetric(
                            testDataSetName: String,
                            inputReader: InputReader,
                            dims: Seq[String],
                            metrics: Seq[String]
                          )